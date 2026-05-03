package com.redismq.common.connection;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.constant.RedisMqKeys;
import com.redismq.common.pojo.Client;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.PushMessage;
import com.redismq.common.pojo.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.common.constant.GlobalConstant.V_QUEUE_SPLITE;
import static com.redismq.common.constant.RedisMQConstant.getClientCollection;
import static com.redismq.common.constant.RedisMQConstant.getGroupCollection;
import static com.redismq.common.constant.RedisMQConstant.getQueueCollection;
import static com.redismq.common.constant.RedisMQConstant.getRebalanceTopic;
import static com.redismq.common.constant.RedisMQConstant.getTopic;
import static com.redismq.common.constant.RedisMQConstant.getVqueueOffsetKey;

@Slf4j
public class RedisMQClientUtil {

    private final RedisClient redisClient;

    public RedisMQClientUtil(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public Queue registerQueue(Queue queue) {
        Set<Queue> allQueue = getQueueList();
        allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queue.getQueueName()))
                .forEach(redisQueue -> redisClient.sRemove(getQueueCollection(), redisQueue));
        redisClient.sAdd(getQueueCollection(), queue);
        return queue;
    }

    public Queue getQueue(String queueName) {
        Set<Queue> allQueue = getQueueList();
        Optional<Queue> first = allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queueName))
                .findFirst();
        return first.orElse(null);
    }

    public Set<Queue> getQueueList() {
        Set<Queue> set = redisClient.sMembers(getQueueCollection(), Queue.class);
        if (CollectionUtils.isEmpty(set)) {
            return new HashSet<>();
        }
        return set.stream().map(s -> (Queue) s).collect(Collectors.toSet());
    }

    public Queue removeQueue(Queue queue) {
        redisClient.sRemove(getQueueCollection(), queue);
        return queue;
    }

    public void registerClient(Client client) {
        redisClient.zAdd(getClientCollection(), client, System.currentTimeMillis());
    }

    public List<Client> getGroupClients() {
        String clientCollection = getClientCollection();
        Map<Client, Double> clientDoubleMap = redisClient.zRangeWithScores(clientCollection, 0, Long.MAX_VALUE,
                Client.class);
        if (CollectionUtils.isEmpty(clientDoubleMap)) {
            return new ArrayList<>();
        }
        String groupId = currentGroupId();
        return clientDoubleMap.entrySet().stream().filter(a -> groupId.equals(a.getKey().getGroupId()))
                .map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public List<Client> getAllClients() {
        String clientCollection = getClientCollection();
        Map<Client, Double> clientDoubleMap = redisClient.zRangeWithScores(clientCollection, 0, Long.MAX_VALUE,
                Client.class);
        return clientDoubleMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public Set<String> getGroups() {
        String groupCollection = getGroupCollection();
        Map<String, Double> doubleMap = redisClient.zRangeWithScores(groupCollection, 0, Long.MAX_VALUE, String.class);
        if (CollectionUtils.isEmpty(doubleMap)) {
            return new HashSet<>();
        }
        return doubleMap.keySet();
    }

    public Long removeClient(double start, double end) {
        return redisClient.zRemoveRangeByScore(getClientCollection(), start, end);
    }

    public void removeClient(Client client) {
        redisClient.zRemove(getClientCollection(), client);
    }

    public void putDeadQueue(Message message) {
        String groupId = currentGroupId();
        String vqueue = StringUtils.defaultIfBlank(message.getVirtualQueueName(), message.getQueue());
        redisClient.zAdd(RedisMqKeys.groupDead(vqueue, groupId), message, System.currentTimeMillis());
    }

    public Boolean removeMessage(String queueName, String msgId) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        String groupId = currentGroupId();
        String lua = "local result={};"
                + "local r1 = redis.call('zrem', KEYS[1], ARGV[1]); "
                + "local r2 = redis.call('hdel', KEYS[2], ARGV[1]); "
                + "table.insert(result, r1); table.insert(result, r2);"
                + "return result;";
        List<String> keys = new ArrayList<>();
        keys.add(RedisMqKeys.groupMessages(vqueue, groupId));
        keys.add(RedisMqKeys.vqueueBody(vqueue));
        List<?> list = redisClient.luaList(lua, keys, new Object[]{msgId});
        boolean success = !CollectionUtils.isEmpty(list)
                && list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count() >= 1;
        if (!success) {
            log.error("remove message failed queueName:{} messageId:{}", vqueue, msgId);
        }
        return success;
    }

    public Boolean removeMessageForAllGroups(String queueName, String msgId) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        List<String> keys = new ArrayList<>(groupMessageKeys(vqueue));
        keys.add(RedisMqKeys.vqueueBody(vqueue));
        String lua = "local removed = 0;"
                + "for i = 1, (#KEYS - 1) do "
                + "    removed = removed + redis.call('zrem', KEYS[i], ARGV[1]); "
                + "end "
                + "removed = removed + redis.call('hdel', KEYS[#KEYS], ARGV[1]); "
                + "return removed;";
        Long removed = redisClient.executeLua(lua, keys, msgId);
        boolean success = removed != null && removed > 0;
        if (!success) {
            log.error("remove message from all groups failed queueName:{} messageId:{}", vqueue, msgId);
        }
        return success;
    }

    public Boolean ackMessage(String queueName, String msgId, long offset) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        return doAck(vqueue, msgId, offset);
    }

    public Boolean ackBatchMessage(String vQueueName, String msgIds, long msgOffset) {
        String vqueue = RedisMqKeys.extractVqueueName(vQueueName);
        return doBatchAck(vqueue, msgIds, msgOffset);
    }

    public List<Pair<Message, Double>> pullMessageByTimeWithScope(String queueName, long pullTime, int startIndex,
            int end) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        Map<Message, Double> messageScopeMap = redisClient.zrangeMessage(RedisMqKeys.vqueueBody(vqueue),
                RedisMqKeys.groupMessages(vqueue, currentGroupId()), pullTime, Double.MAX_VALUE, startIndex, end);
        List<Pair<Message, Double>> pairs = new ArrayList<>();
        messageScopeMap.forEach((k, v) -> pairs.add(Pair.of(k, v)));
        return pairs;
    }

    public List<Message> pullMessage(String queueName, long minScopre, long time, int start, int pullSize) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        Map<Message, Double> messageScopeMap = redisClient.zrangeMessage(RedisMqKeys.vqueueBody(vqueue),
                RedisMqKeys.groupMessages(vqueue, currentGroupId()), minScopre, time, start, pullSize);
        if (CollectionUtils.isEmpty(messageScopeMap)) {
            return new ArrayList<>();
        }
        return new ArrayList<>(messageScopeMap.keySet());
    }

    public List<Pair<Message, Double>> pullMessageWithScope(String queueName, int start, int pullSize) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        Set<String> groups = getGroups();
        if (CollectionUtils.isEmpty(groups) || pullSize <= 0) {
            return new ArrayList<>();
        }
        int fetchSize = Math.max(start + pullSize, 1) * Math.max(groups.size(), 1);
        Map<String, Pair<Message, Double>> mergedMessages = new HashMap<>(fetchSize);
        for (String groupId : groups) {
            Map<Message, Double> messageScopeMap = redisClient.zrangeMessage(RedisMqKeys.vqueueBody(vqueue),
                    RedisMqKeys.groupMessages(vqueue, groupId), 0, Double.MAX_VALUE, 0, fetchSize);
            messageScopeMap.forEach((message, score) -> {
                if (message == null || StringUtils.isBlank(message.getId())) {
                    return;
                }
                Pair<Message, Double> current = mergedMessages.get(message.getId());
                if (current == null || score < current.getRight()) {
                    mergedMessages.put(message.getId(), Pair.of(message, score));
                }
            });
        }
        if (mergedMessages.isEmpty()) {
            return new ArrayList<>();
        }
        return mergedMessages.values().stream().sorted(Comparator.comparingDouble(Pair::getRight)).skip(start)
                .limit(pullSize).collect(Collectors.toList());
    }

    public Boolean unlock(String key) {
        return redisClient.unlock(key);
    }

    public Long queueSize(String vQueue) {
        String vqueue = RedisMqKeys.extractVqueueName(vQueue);
        Long size = redisClient.executeLua("return redis.call('hlen', KEYS[1]);",
                Collections.singletonList(RedisMqKeys.vqueueBody(vqueue)));
        return size == null ? 0L : size;
    }

    public Long deadQueueSize(String queueName) {
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        Long size = redisClient.zSize(RedisMqKeys.groupDead(vqueue, currentGroupId()));
        return size == null ? 0L : size;
    }

    public Boolean lock(String key, Duration duration) {
        return redisClient.lock(key, "", duration);
    }

    public void publishRebalance(String clientId) {
        redisClient.convertAndSend(getRebalanceTopic(currentGroupId()), clientId);
    }

    public void publishRebalance(String groupId, String clientId) {
        redisClient.convertAndSend(getRebalanceTopic(groupId), clientId);
    }

    public void publishPullMessage(PushMessage pushMessage) {
        redisClient.convertAndSend(getTopic(), pushMessage);
    }

    public void publish(String topic, Object obj) {
        redisClient.convertAndSend(topic, obj);
    }

    public Long executeLua(String lua, List<String> keys, Object... args) {
        return redisClient.executeLua(lua, keys, args);
    }

    public Long getQueueMaxOffset(String vQueueName) {
        Object obj = redisClient.get(getVqueueOffsetKey(vQueueName));
        if (obj == null) {
            return 0L;
        }
        return Long.parseLong(obj.toString());
    }

    public Boolean isLock(String key) {
        return redisClient.isLock(key);
    }

    public void registerGroup() {
        redisClient.zAdd(getGroupCollection(), currentGroupId(), System.currentTimeMillis());
    }

    public void registerQueueGroup(String queueName) {
        String groupId = currentGroupId();
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        redisClient.sAdd(RedisMqKeys.groupVqueues(groupId), vqueue);
        redisClient.sAdd(RedisMqKeys.vqueueGroups(vqueue), groupId);
    }

    public void registerMessageGroups(String queueName, Set<String> groupIds) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return;
        }
        String vqueue = RedisMqKeys.extractVqueueName(queueName);
        redisClient.sAdd(RedisMqKeys.vqueueGroups(vqueue), groupIds.toArray());
        for (String groupId : groupIds) {
            redisClient.sAdd(RedisMqKeys.groupVqueues(groupId), vqueue);
        }
    }

    public Map<String, Double> getQueueGroupOffsets(String groupIdOrIndexKey) {
        String groupId = resolveGroupId(groupIdOrIndexKey);
        Set<String> vqueues = getGroupVqueues(groupId);
        Map<String, Double> result = new HashMap<>(vqueues.size());
        for (String vqueue : vqueues) {
            result.put(vqueue, (double) getQueueGroupOffset(groupId, vqueue));
        }
        return result;
    }

    public Long getQueueGroupOffset(String groupIdOrIndexKey, String queue) {
        String groupId = resolveGroupId(groupIdOrIndexKey);
        Object value = redisClient.get(RedisMqKeys.groupOffset(queue, groupId));
        if (value == null) {
            return 0L;
        }
        return Long.parseLong(value.toString());
    }

    public void deleteGroup(String groupId) {
        Set<String> vqueues = collectGroupVqueues(groupId);
        Set<String> deleteKeys = new LinkedHashSet<>();
        deleteKeys.add(RedisMqKeys.groupVqueues(groupId));
        for (String vqueue : vqueues) {
            deleteKeys.add(RedisMqKeys.groupOffset(vqueue, groupId));
            deleteKeys.add(RedisMqKeys.groupMessages(vqueue, groupId));
            deleteKeys.add(RedisMqKeys.groupDead(vqueue, groupId));
            deleteKeys.add(RedisMqKeys.groupDeadBody(vqueue, groupId));
            redisClient.sRemove(RedisMqKeys.vqueueGroups(vqueue), groupId);
        }
        if (!deleteKeys.isEmpty()) {
            redisClient.delete(deleteKeys);
        }
        redisClient.zRemove(getGroupCollection(), groupId);
    }

    public Set<String> getGroupVqueues(String groupId) {
        Set<String> members = redisClient.sMembers(RedisMqKeys.groupVqueues(groupId), String.class);
        if (CollectionUtils.isEmpty(members)) {
            return new HashSet<>();
        }
        return members;
    }

    private Boolean doAck(String vqueue, String msgId, long offset) {
        String groupId = currentGroupId();
        String otherQueueKeys = otherGroupMessageKeys(vqueue, groupId);
        String lua = "local result={};\n"
                + "local currentOffset = redis.call('get', KEYS[4]);\n"
                + "local currentOffsetNumber = currentOffset and tonumber(currentOffset) or 0;\n"
                + "local msgOffset = tonumber(ARGV[2]);\n"
                + "local r1 = redis.call('zrem', KEYS[1], ARGV[1]);\n"
                + "table.insert(result, r1);\n"
                + "if msgOffset and msgOffset > currentOffsetNumber then\n"
                + "    redis.call('set', KEYS[4], msgOffset);\n"
                + "    currentOffsetNumber = msgOffset;\n"
                + "end\n"
                + "local groupAck = true;\n"
                + "for queueGroup in KEYS[3]:gmatch('([^,]+)') do\n"
                + "    local res = redis.call('zscore', queueGroup, ARGV[1]);\n"
                + "    if res then\n"
                + "        groupAck = false;\n"
                + "    end\n"
                + "end\n"
                + "if groupAck then\n"
                + "    local r2 = redis.call('hdel', KEYS[2], ARGV[1]);\n"
                + "    table.insert(result, r2);\n"
                + "end\n"
                + "local size = redis.call('hlen', KEYS[2]);\n"
                + "if size > 0 then\n"
                + "    for msgQueue in KEYS[3]:gmatch('([^,]+)') do\n"
                + "        local data = redis.call('zrangebyscore', msgQueue, 0, currentOffsetNumber - (tonumber(KEYS[5]) - 1));\n"
                + "        for i, messageId in ipairs(data) do\n"
                + "            redis.call('zrem', msgQueue, messageId);\n"
                + "            local r3 = redis.call('hdel', KEYS[2], messageId);\n"
                + "            table.insert(result, r3);\n"
                + "        end\n"
                + "    end\n"
                + "end\n"
                + "return result;";
        List<String> keys = new ArrayList<>();
        keys.add(RedisMqKeys.groupMessages(vqueue, groupId));
        keys.add(RedisMqKeys.vqueueBody(vqueue));
        keys.add(otherQueueKeys);
        keys.add(RedisMqKeys.groupOffset(vqueue, groupId));
        keys.add(GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax().toString());
        List<?> list = redisClient.luaList(lua, keys, new Object[]{msgId, offset});
        boolean success = !CollectionUtils.isEmpty(list)
                && list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count() >= 1;
        if (!success) {
            log.error("remove message failed queueName:{} messageId:{}", vqueue, msgId);
        }
        return success;
    }

    private Boolean doBatchAck(String vqueue, String msgIds, long msgOffset) {
        String groupId = currentGroupId();
        String otherQueueKeys = otherGroupMessageKeys(vqueue, groupId);
        String lua = "local result={};\n"
                + "local msgIds = ARGV[1];\n"
                + "local msgOffset = tonumber(ARGV[2]);\n"
                + "local currentOffset = redis.call('get', KEYS[4]);\n"
                + "local currentOffsetNumber = currentOffset and tonumber(currentOffset) or 0;\n"
                + "for msgId in msgIds:gmatch('([^,]+)') do\n"
                + "    local r1 = redis.call('zrem', KEYS[1], msgId);\n"
                + "    table.insert(result, r1);\n"
                + "    local groupAck = true;\n"
                + "    for queueGroup in KEYS[3]:gmatch('([^,]+)') do\n"
                + "        local res = redis.call('zscore', queueGroup, msgId);\n"
                + "        if res then\n"
                + "            groupAck = false;\n"
                + "        end\n"
                + "    end\n"
                + "    if groupAck then\n"
                + "        local r2 = redis.call('hdel', KEYS[2], msgId);\n"
                + "        table.insert(result, r2);\n"
                + "    end\n"
                + "end\n"
                + "if msgOffset and msgOffset > currentOffsetNumber then\n"
                + "    redis.call('set', KEYS[4], msgOffset);\n"
                + "    currentOffsetNumber = msgOffset;\n"
                + "end\n"
                + "local size = redis.call('hlen', KEYS[2]);\n"
                + "if size > 0 then\n"
                + "    for msgQueue in KEYS[3]:gmatch('([^,]+)') do\n"
                + "        local data = redis.call('zrangebyscore', msgQueue, 0, currentOffsetNumber - (tonumber(KEYS[5]) - 1));\n"
                + "        for i, messageId in ipairs(data) do\n"
                + "            redis.call('zrem', msgQueue, messageId);\n"
                + "            local r3 = redis.call('hdel', KEYS[2], messageId);\n"
                + "            table.insert(result, r3);\n"
                + "        end\n"
                + "    end\n"
                + "end\n"
                + "return result;";
        List<String> keys = new ArrayList<>();
        keys.add(RedisMqKeys.groupMessages(vqueue, groupId));
        keys.add(RedisMqKeys.vqueueBody(vqueue));
        keys.add(otherQueueKeys);
        keys.add(RedisMqKeys.groupOffset(vqueue, groupId));
        keys.add(GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax().toString());
        List<?> list = redisClient.luaList(lua, keys, new Object[]{msgIds, msgOffset});
        boolean success = !CollectionUtils.isEmpty(list)
                && list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count() >= 1;
        if (!success) {
            log.error("remove message failed queueName:{} messageIds:{}", vqueue, msgIds);
        }
        return success;
    }

    private String otherGroupMessageKeys(String vqueue, String currentGroupId) {
        return getMessageGroups(vqueue).stream().filter(groupId -> !groupId.equals(currentGroupId))
                .map(groupId -> RedisMqKeys.groupMessages(vqueue, groupId)).collect(Collectors.joining(","));
    }

    private List<String> groupMessageKeys(String vqueue) {
        return getMessageGroups(vqueue).stream().map(groupId -> RedisMqKeys.groupMessages(vqueue, groupId))
                .collect(Collectors.toList());
    }

    private Set<String> getMessageGroups(String vqueue) {
        Set<String> groups = new LinkedHashSet<>(getGroups());
        Set<String> indexedGroups = redisClient.sMembers(RedisMqKeys.vqueueGroups(vqueue), String.class);
        if (!CollectionUtils.isEmpty(indexedGroups)) {
            groups.addAll(indexedGroups);
        }
        return groups;
    }

    private Set<String> collectGroupVqueues(String groupId) {
        Set<String> vqueues = new LinkedHashSet<>(getGroupVqueues(groupId));
        for (Queue queue : getQueueList()) {
            Integer virtual = queue.getVirtual();
            int virtualCount = virtual == null || virtual <= 0 ? 1 : virtual;
            for (int i = 0; i < virtualCount; i++) {
                vqueues.add(queue.getQueueName() + V_QUEUE_SPLITE + i);
            }
        }
        return vqueues;
    }

    private String resolveGroupId(String groupIdOrIndexKey) {
        if (StringUtils.isBlank(groupIdOrIndexKey)) {
            return currentGroupId();
        }
        if (!groupIdOrIndexKey.contains(":")) {
            return groupIdOrIndexKey;
        }
        String marker = ":group:";
        int start = groupIdOrIndexKey.indexOf(marker);
        if (start < 0) {
            return StringUtils.substringAfterLast(groupIdOrIndexKey, ":");
        }
        int groupStart = start + marker.length();
        String suffix = ":vqueues";
        int end = groupIdOrIndexKey.indexOf(suffix, groupStart);
        if (end < 0) {
            return StringUtils.substring(groupIdOrIndexKey, groupStart);
        }
        return StringUtils.substring(groupIdOrIndexKey, groupStart, end);
    }

    private String currentGroupId() {
        if (GlobalConfigCache.CONSUMER_CONFIG == null
                || StringUtils.isBlank(GlobalConfigCache.CONSUMER_CONFIG.getGroupId())) {
            throw new IllegalStateException("RedisMQ groupId 未初始化");
        }
        return GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
    }
}
