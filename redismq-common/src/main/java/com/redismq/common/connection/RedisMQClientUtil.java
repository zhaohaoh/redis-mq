package com.redismq.common.connection;


import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.RedisMQConstant;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.common.constant.RedisMQConstant.getClientCollection;
import static com.redismq.common.constant.RedisMQConstant.getGroupCollection;
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
        redisClient.zAdd(getClientCollection(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()), client, heartbeatTime);
    }
    
    
    /**
     * 组内所有客户端
     *
     * @return {@link Set}<{@link String}>
     */
    public List<Client> getGroupClients() {
        String clientCollection = getClientCollection(GlobalConfigCache.CONSUMER_CONFIG.getGroupId());
        Map<Client, Double> clientDoubleMap = redisClient
                .zRangeWithScores(clientCollection, 0, Long.MAX_VALUE, Client.class);
        if (CollectionUtils.isEmpty(clientDoubleMap)) {
            return new ArrayList<>();
        }
        return clientDoubleMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }
    
    /**
     * 所有客户端
     *
     * @return {@link Set}<{@link String}>
     */
    public List<Client> getAllClients() {
        Set<String> groups = getGroups();
        if (CollectionUtils.isEmpty(groups)){
            return new ArrayList<>();
        }
        Map<Client, Double> clientDoubleMap =new HashMap<>();
        groups.forEach((k)->{
            String clientCollection = getClientCollection(k);
            Map<Client, Double> clientDoubleMapT = redisClient
                    .zRangeWithScores(clientCollection, 0, Long.MAX_VALUE, Client.class);
            if (!CollectionUtils.isEmpty(clientDoubleMap)) {
                clientDoubleMap.putAll(clientDoubleMapT);
            }else{
                redisClient.zRemove(clientCollection,k);
            }
        });
    
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
        return redisClient.zRemoveRangeByScore(getClientCollection(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()), start, end);
    }
    
    /**
     * 删除客户端
     */
    public void removeClient(String clientId) {
        redisClient.zRemove(getClientCollection(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()), clientId);
    }
    /**
     * 删除客户端
     */
    public void removeClient(String groupId,String clientId) {
        if (StringUtils.isBlank(groupId)){
            Set<String> groups = getGroups();
            for (String group : groups) {
                redisClient.zRemove(getClientCollection(group), clientId);
            }
        }else{
            redisClient.zRemove(getClientCollection(groupId), clientId);
        }
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
        queueName= RedisMQConstant.getVQueueNameByVQueue(queueName);
        params.add(queueName);
        params.add(queueName + ":body");
        Object[] objects = {msgId};
        List list = redisClient.luaList(lua, params, objects);
        if (!CollectionUtils.isEmpty(list)){
            long count = list.stream().mapToInt(value -> Integer.parseInt(value.toString())).count();
            success = count >= 2;
        }
        if (!success){
            log.error("remove message failed, queueName:{} messageId:{}",queueName,msgId);
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
                .zrangeMessage(queueName, pullTime, Double.MAX_VALUE, startIndex, end);
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
        queueName= RedisMQConstant.getVQueueNameByVQueue(queueName);
        Map<Message, Double> messageScopeMap = redisClient.zrangeMessage(queueName, minScopre, time, start, pullSize);
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
                .zrangeMessage(queueName, 0, Double.MAX_VALUE, start, pullSize);
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
    
    public Boolean isLock(String key) {
        return redisClient.isLock(key);
    }
    
    public void registerGroup() {
        redisClient.zAdd(getGroupCollection(),GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),System.currentTimeMillis());
    }
}
