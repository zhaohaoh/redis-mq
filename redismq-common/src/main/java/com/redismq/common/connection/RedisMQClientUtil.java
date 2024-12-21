package com.redismq.common.connection;


import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.pojo.Client;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.PushMessage;
import com.redismq.common.pojo.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.GlobalConstant.V_QUEUE_SPLITE;
import static com.redismq.common.constant.RedisMQConstant.NAMESPACE;
import static com.redismq.common.constant.RedisMQConstant.PREFIX;
import static com.redismq.common.constant.RedisMQConstant.getClientCollection;
import static com.redismq.common.constant.RedisMQConstant.getGroupCollection;
import static com.redismq.common.constant.RedisMQConstant.getOffsetGroupCollection;
import static com.redismq.common.constant.RedisMQConstant.getQueueCollection;
import static com.redismq.common.constant.RedisMQConstant.getRebalanceTopic;
import static com.redismq.common.constant.RedisMQConstant.getTopic;

@Slf4j
public class RedisMQClientUtil {
    
    private final RedisClient redisClient;
    
    public RedisMQClientUtil(RedisClient redisClient) {
        this.redisClient = redisClient;
    }
    
    /**
     * 注册队列
     */
    public Queue registerQueue(Queue queue) {
        Set<Queue> allQueue = getQueueList();
        allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queue.getQueueName()))
                .forEach(redisQueue -> redisClient.sRemove(getQueueCollection(), redisQueue));
        redisClient.sAdd(getQueueCollection(), queue);
        return queue;
    }
    
    /**
     * 获取队列
     */
    public Queue getQueue(String queueName) {
        Set<Queue> allQueue = getQueueList();
        Optional<Queue> first = allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queueName))
                .findFirst();
        return first.orElse(null);
    }
    
    /**
     * 获取所有队列
     */
    public Set<Queue> getQueueList() {
        Set<Queue> set = redisClient.sMembers(getQueueCollection(), Queue.class);
        if (CollectionUtils.isEmpty(set)) {
            return new HashSet<>();
        }
        return set.stream().map(s -> (Queue) s).collect(Collectors.toSet());
    }
    
    /**
     * 注册队列
     */
    public Queue removeQueue(Queue queue) {
        redisClient.sRemove(getQueueCollection(), queue);
        return queue;
    }
    
    public void registerClient(Client client) {
        long heartbeatTime = System.currentTimeMillis();
        redisClient.zAdd(getClientCollection(), client, heartbeatTime);
    }
    
    
    /**
     * 组内所有客户端
     *
     * @return {@link Set}<{@link String}>
     */
    public List<Client> getGroupClients() {
        String clientCollection = getClientCollection();
        Map<Client, Double> clientDoubleMap = redisClient
                .zRangeWithScores(clientCollection, 0, Long.MAX_VALUE, Client.class);
        if (CollectionUtils.isEmpty(clientDoubleMap)) {
            return new ArrayList<>();
        }
        return clientDoubleMap.entrySet().stream().filter(a->a.getKey().getGroupId().equals(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()))
                .map(Map.Entry::getKey).collect(Collectors.toList());
    }
    
    /**
     * 所有客户端
     *
     * @return {@link Set}<{@link String}>
     */
    public List<Client> getAllClients() {
        
        String clientCollection = getClientCollection();
        Map<Client, Double> clientDoubleMap = redisClient.zRangeWithScores(clientCollection, 0, Long.MAX_VALUE,
                Client.class);
        
        return clientDoubleMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }
    
    /**
     * 获取所有的group
     * @return
     */
    public Set<String> getGroups() {
        String groupCollection = getGroupCollection();
        Map<String, Double> doubleMap = redisClient.zRangeWithScores(groupCollection, 0, Long.MAX_VALUE,
                String.class);
         if (CollectionUtils.isEmpty(doubleMap)){
           return new HashSet<>();
          }
        return  doubleMap.keySet();
        
    }
    
    
    /**
     * 删除客户端
     */
    public Long removeClient(double start, double end) {
        return redisClient.zRemoveRangeByScore(getClientCollection(), start, end);
    }
    
    /**
     * 删除客户端
     */
    public void removeClient(Client client) {
        redisClient.zRemove(getClientCollection(), client);
    }
    
    /**
     * 放入死队列
     */
    public void putDeadQueue(Message message) {
        String queue = message.getQueue();
        String deadQueue = RedisMQConstant.getDeadQueueNameByQueue(queue);
        redisClient.zAdd(deadQueue, message, System.currentTimeMillis());
    }
    
    /**
     * 删除消息
     */
    public Boolean removeMessage(String queueName, String msgId) {
        boolean success =false;
        String lua = "local result={};"
                + "local r1 = redis.call('zrem', KEYS[1], ARGV[1]); "
                + "local r2  = redis.call('hdel', KEYS[2],  ARGV[1]); "
                + "table.insert(result,r1);table.insert(result,r2);"
                + "return result;";
        List<String> params=new ArrayList<>();
        String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
        queueName= RedisMQConstant.getVQueueNameByVQueue(queueName);
        //队列名+groupId
        params.add(queueName + SPLITE + groupId);
        params.add(queueName + ":body");
        Object[] objects = {msgId};
        List list = redisClient.luaList(lua, params, objects);
        if (!CollectionUtils.isEmpty(list)){
            long count = list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count();
            success = count >= 1;
        }
        if (!success){
            log.error("remove message failed, queueName:{} messageId:{}",queueName,msgId);
        }
        return success;
    }
    
    /**
     * ack消息
     */
    public Boolean ackMessage(String queueName, String msgId,long offset) {
        boolean success =false;
        String lua = "local result={};\n" + "local messageIdQueue=KEYS[1];\n" + "local queueGroups= KEYS[3];\n"
                + "local offsetGroup= KEYS[4];\n" + "local orginalQueueName =KEYS[5];\n"
                + "local otherMessageIdQueues =KEYS[6];\n" + "local diffMax =KEYS[7];\n" + "local msgOffset =KEYS[8];\n"
                + "\n" + "local r1 = redis.call('zrem', messageIdQueue, ARGV[1]);\n" + "table.insert(result,r1);\n"
                + "\n" + "local currentOffset = redis.call('ZSCORE', offsetGroup,orginalQueueName);\n"
                + "if currentOffset and msgOffset and  tonumber(msgOffset) > tonumber(currentOffset) then\n"
                + "    redis.call('zadd', offsetGroup, msgOffset,orginalQueueName);\n" + "end\n" + "\n"
                + "local groupAck = true;\n" + "for queueGroup in queueGroups:gmatch(\"([^,]+)\") do\n"
                + "    local res = redis.call('ZSCORE', queueGroup, ARGV[1]);\n" + "    if res then\n"
                + "        groupAck = false;\n" + "    end\n" + "end\n" + "\n" + "if groupAck then\n"
                + "    local r2  = redis.call('hdel', KEYS[2],  ARGV[1]);\n" + "    table.insert(result,r2);\n"
                + "end\n" + "local size = redis.call('HLEN',KEYS[2]);\n" + "if size>0 then\n"
                + "    for msgQueue in otherMessageIdQueues:gmatch(\"([^,]+)\") do\n"
                + "        local  data = redis.call('ZRANGEBYSCORE',msgQueue,0,currentOffset - (tonumber(diffMax)-1));\n"
                + "        for i, messageId in ipairs(data) do\n"
                + "            redis.call('zrem',msgQueue,messageId);\n"
                + "            local r2  = redis.call('hdel', KEYS[2],  messageId);\n"
                + "            table.insert(result,r2);\n" + "        end\n" + "    end\n" + "end\n" + "return result;";
        List<String> keys = new ArrayList<>();
        String orginalQueueName = RedisMQConstant.getQueueNameByVirtual(queueName);
        
        queueName = RedisMQConstant.getVQueueNameByVQueue(queueName);
        String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
        Set<String> group =  getGroups();
        String finalQueueName = queueName;
        //其他group的Id 判断所有groupId的偏移量都提交后。才删除消息体
        String queueGroups = group.stream()
                .filter(a->!a.equals(groupId))
                .map(g -> finalQueueName + SPLITE + g)
                .collect(Collectors.joining(","));
        //1.队列名+groupId  存消息的队列
        keys.add(queueName + SPLITE + groupId);
        //2.消息体key
        keys.add(queueName + ":body");
        //3.当前的所有groupName
        keys.add(queueGroups);
        String offsetGroupName = getOffsetGroupCollection(groupId);
        //4 维护当前队列偏移量的组名
        keys.add(offsetGroupName);
        //5.原始队列名称
        keys.add(orginalQueueName);
        //key6 除当前队列外所有其他队列的消息id队列
        String offsetGroups = group.stream().filter(gId -> !gId.equals(groupId))
                .map(gId ->  finalQueueName + SPLITE + gId).collect(Collectors.joining(","));
        keys.add(offsetGroups);
        //key7 除当前队列外所有其他队列的消息id队列
        keys.add(GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax().toString());
     
        
        Object[] objects = {msgId,offset};
        List list = redisClient.luaList(lua, keys, objects);
        if (!CollectionUtils.isEmpty(list)){
            long count = list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count();
            success = count >= 1;
        }
        if (!success){
            log.error("remove message failed, queueName:{} messageId:{}",queueName,msgId);
        }
        return success;
    }
    
    
    /**
     * ack消息
     */
    public Boolean ackBatchMessage(String queueName, String msgIds,long msgOffset) {
        boolean success =false;
        String lua = "local result={};\n" + "local messageIdQueue=KEYS[1];\n" + "local queueGroups= KEYS[3];\n"
                + "local offsetGroup= KEYS[4];\n" + "local orginalQueueName =KEYS[5];\n"
                + "local otherMessageIdQueues =KEYS[6];\n" + "local diffMax =KEYS[7];\n" + "\n"
                + "local msgIds = ARGV[1];\n" + "local msgOffset = ARGV[2];\n" + "\n"
                + "local currentOffset = redis.call('ZSCORE', offsetGroup,orginalQueueName);\n" + "\n"
                + "for msgId in msgIds:gmatch(\"([^,]+)\") do\n"
                + "local r1 = redis.call('zrem', messageIdQueue, msgId);\n" + "table.insert(result,r1);\n" + "\n"
                + "local groupAck = true;\n" + "for queueGroup in queueGroups:gmatch(\"([^,]+)\") do\n"
                + "    local res = redis.call('ZSCORE', queueGroup, msgId);\n" + "    if res then\n"
                + "        groupAck = false;\n" + "    end\n" + "end\n" + "\n" + "if groupAck then\n"
                + "    local r2  = redis.call('hdel', KEYS[2],  msgId);\n" + "    table.insert(result,r2);\n" + "end\n"
                + "local size = redis.call('HLEN',KEYS[2]);\n" + "if size>0 then\n"
                + "    for msgQueue in otherMessageIdQueues:gmatch(\"([^,]+)\") do\n"
                + "        local  data = redis.call('ZRANGEBYSCORE',msgQueue,0,currentOffset - (tonumber(diffMax)-1));\n"
                + "        for i, messageId in ipairs(data) do\n"
                + "            redis.call('zrem',msgQueue,messageId);\n"
                + "            local r2  = redis.call('hdel', KEYS[2],  messageId);\n"
                + "            table.insert(result,r2);\n" + "        end\n" + "    end\n" + "end\n" + "end\n" + "\n"
                + "if currentOffset and msgOffset and  (tonumber(msgOffset) > tonumber(currentOffset)) then\n"
                + "    redis.call('zadd', offsetGroup, msgOffset,orginalQueueName);\n" + "end\n" + "\n"
                + "return result;\n";
        List<String> keys = new ArrayList<>();
        String orginalQueueName = RedisMQConstant.getQueueNameByVirtual(queueName);
        
        queueName = RedisMQConstant.getVQueueNameByVQueue(queueName);
        String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
        Set<String> group =  getGroups();
        String finalQueueName = queueName;
        //其他group的Id 判断所有groupId的偏移量都提交后。才删除消息体
        String queueGroups = group.stream()
                .filter(a->!a.equals(groupId))
                .map(g -> finalQueueName + SPLITE + g)
                .collect(Collectors.joining(","));
        //1.队列名+groupId  存消息的队列
        keys.add(queueName + SPLITE + groupId);
        //2.消息体key
        keys.add(queueName + ":body");
        //3.当前的所有groupName
        keys.add(queueGroups);
        String offsetGroupName = getOffsetGroupCollection(groupId);
        //4 维护当前队列偏移量的组名
        keys.add(offsetGroupName);
        //5.原始队列名称
        keys.add(orginalQueueName);
        //key6 除当前队列外所有其他队列的消息id队列
        String offsetGroups = group.stream().filter(gId -> !gId.equals(groupId))
                .map(gId ->  finalQueueName + SPLITE + gId).collect(Collectors.joining(","));
        keys.add(offsetGroups);
        //key7 除当前队列外所有其他队列的消息id队列
        keys.add(GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax().toString());
        
        //msgOffset 所有消息最大偏移量
        Object[] objects = {msgIds,msgOffset};
        List list = redisClient.luaList(lua, keys, objects);
        if (!CollectionUtils.isEmpty(list)){
            long count = list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count();
            success = count >= 1;
        }
        if (!success){
            log.error("remove message failed, queueName:{} messageIds:{}",queueName,msgIds);
        }
        return success;
    }
    
    /**
     * 根据时间拉取队列中的消息
     */
    public List<Pair<Message, Double>> pullMessageByTimeWithScope(String queueName, long pullTime, int startIndex,
            int end) {
        queueName= RedisMQConstant.getVQueueNameByVQueue(queueName);
     
        Map<Message, Double> messageScopeMap = redisClient
                .zrangeMessage(queueName,GlobalConfigCache.CONSUMER_CONFIG.getGroupId(), pullTime, Double.MAX_VALUE, startIndex, end);
        List<Pair<Message, Double>> pairs = new ArrayList<>();
        messageScopeMap.forEach((k, v) -> {
            pairs.add( Pair.of(k, v));
        });
        return pairs;
    }
    
    /**
     * 拉取队列中的消息 拉取可以消费的消息
     */
    public List<Message> pullMessage(String queueName,long  minScopre,long time, int start, int pullSize) {
        queueName = RedisMQConstant.getVQueueNameByVQueue(queueName);
        Map<Message, Double> messageScopeMap = redisClient.zrangeMessage(queueName,GlobalConfigCache.CONSUMER_CONFIG.getGroupId(), minScopre, time, start, pullSize);
        if (CollectionUtils.isEmpty(messageScopeMap)) {
            return new ArrayList<>();
        }
        return new ArrayList<>(messageScopeMap.keySet());
    }
    
    /**
     * 拉取队列中的消息 不管消息的scope消费时间
     */
    public List<Pair<Message, Double>> pullMessageWithScope(String queueName, int start, int pullSize) {
        queueName= RedisMQConstant.getVQueueNameByVQueue(queueName);
        Map<Message, Double> messageScopeMap = redisClient
                .zrangeMessage(queueName, GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),0, Double.MAX_VALUE, start, pullSize);
        if (CollectionUtils.isEmpty(messageScopeMap)) {
            return new ArrayList<>();
        }
        List<Pair<Message, Double>> pairs = new ArrayList<>();
        messageScopeMap.forEach((k, v) -> {
            pairs.add(Pair.of(k, v));
        });
        return pairs;
    }
    
    
    /**
     * 删除指定key
     */
    public Boolean unlock(String key) {
        return redisClient.unlock(key);
    }
    
    /**
     * 队列大小
     */
    public Long queueSize(String vQueue) {
        String queueName = RedisMQConstant.getVQueueNameByVQueue(vQueue);
        return redisClient.zSize(queueName);
    }
    
    /**
     * 锁定指定key
     */
    public Boolean lock(String key, Duration duration) {
        return redisClient.lock(key, "", duration);
    }
    
    /**
     * 发布重新平衡
     *
     * @param clientId 客户端id
     */
    public void publishRebalance(String clientId) {
        redisClient.convertAndSend(getRebalanceTopic(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()), clientId);
    }
    
    /**
     * 发布重新平衡
     *
     * @param clientId 客户端id
     */
    public void publishRebalance(String groupId,String clientId) {
        redisClient.convertAndSend(getRebalanceTopic(groupId), clientId);
    }
    
    /**
     * 发布拉取消息的topic
     */
    public void publishPullMessage(PushMessage pushMessage) {
        redisClient.convertAndSend(getTopic(), pushMessage);
    }
    
    /**
     * 发布topic
     */
    public void publish(String topic, Object obj) {
        redisClient.convertAndSend(topic, obj);
    }
    
    public Long executeLua(String lua, List<String> keys, Object... args) {
        return redisClient.executeLua(lua, keys, args);
    }
    
    public Long getQueueMaxOffset(String queueName){
        String queueOffset = PREFIX + NAMESPACE + SPLITE + "QUEUE_OFFSET" + SPLITE + queueName;
        Object obj = redisClient.get(queueOffset);
        if (obj==null){
            return 0L;
        }
        String str = (String) obj;
        return Long.parseLong(str);
    }
    
    public Boolean isLock(String key) {
        return redisClient.isLock(key);
    }
    
    /**
     * 注册所有的消费组
     */
    public void registerGroup() {
        redisClient.zAdd(getGroupCollection(),GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),System.currentTimeMillis());
    }
    
    /**
     * 注册所有的消费组
     */
    public void registerQueueGroup(String queueName) {
        // scope等于偏移量
        String offsetGroupCollection = getOffsetGroupCollection(GlobalConfigCache.CONSUMER_CONFIG.getGroupId());
        Boolean absent = redisClient.zAddIfAbsent(offsetGroupCollection,
                queueName, 0L);
        
    }
    
    public Map<String, Double> getQueueGroupOffsets(String offsetGroupCollection) {
        Map<String, Double> queueOffsetMap = redisClient.zRangeWithScores(offsetGroupCollection, 0, Long.MAX_VALUE,
                String.class);
        
        return queueOffsetMap;
    }
    
    public  Long getQueueGroupOffset(String offsetGroupCollection,String queue) {
        Double offset = redisClient.zScore(offsetGroupCollection, queue);
        if (offset==null){
            return 0L;
        }
        return offset.longValue();
    }
    
    public void deleteGroup(String groupId) {
        redisClient.zRemove(getGroupCollection(),groupId);
        String offsetGroupCollection = getOffsetGroupCollection(groupId);
        redisClient.delete(offsetGroupCollection);
        //查找所有队列的虚拟队列。删除队列内的消息
        Set<Queue> queueList = getQueueList();
        for (Queue queue : queueList) {
            Integer virtual = queue.getVirtual();
            for (Integer i = 0; i < virtual; i++) {
                String vQueueName = queue.getQueueName() + V_QUEUE_SPLITE + i;
                //队列的分组
                redisClient.delete(vQueueName + groupId);
            }
        }
        
    }
}
