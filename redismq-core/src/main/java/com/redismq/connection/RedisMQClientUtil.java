package com.redismq.connection;


import com.redismq.Message;
import com.redismq.constant.RedisMQConstant;
import com.redismq.pojo.Client;
import com.redismq.queue.Queue;
import javafx.util.Pair;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.constant.RedisMQConstant.getClientCollection;
import static com.redismq.constant.RedisMQConstant.getQueueCollection;
import static com.redismq.constant.RedisMQConstant.getRebalanceTopic;

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
        Set<Object> set = redisClient.sMembers(getQueueCollection());
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
    
    public void registerClient(String clientId, String applicationName) {
        Client client = new Client();
        client.setClientId(clientId);
        client.setApplicationName(applicationName);
        long heartbeatTime = System.currentTimeMillis();
        redisClient.zAdd(getClientCollection(), client, heartbeatTime);
    }
    
    /**
     * 所有客户端
     *
     * @return {@link Set}<{@link String}>
     */
    public List<Client> getClients() {
        String clientCollection = getClientCollection();
        Set<ZSetOperations.TypedTuple<Object>> typedTuples = redisClient
                .zRangeWithScores(clientCollection, 0, Long.MAX_VALUE);
        if (CollectionUtils.isEmpty(typedTuples)) {
            return new ArrayList<>();
        }
        List<Client> clients = typedTuples.stream().map(a -> (Client) a.getValue()).collect(Collectors.toList());
        return clients;
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
    public void removeClient(String clientId) {
        redisClient.zRemove(getClientCollection(), clientId);
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
    public Long removeMessage(String queueName, Message message) {
        return redisClient.zRemove(queueName, message);
    }
    
    
    /**
     * 拉取队列中的需要立即消费的消息
     */
    public List<Message> pullMessage(String queueName, long pullTime,int startIndex, int pullSize) {
        Set<ZSetOperations.TypedTuple<Object>> headDatas = redisClient
                .zRangeByScoreWithScores(queueName, 0, pullTime, startIndex, pullSize);
        if (CollectionUtils.isEmpty(headDatas)) {
            return new ArrayList<>();
        }
        List<Message> messages = new ArrayList<>();
        headDatas.forEach(s -> messages.add((Message) s.getValue()));
        return messages;
    }
    
    /**
     * 拉取队列中的消息 不管消息的scope消费时间
     */
    public List<Pair<Message, Double>> pullMessage(String queueName, int start, int pullSize) {
        Set<ZSetOperations.TypedTuple<Object>> headDatas = redisClient.zRangeWithScores(queueName, start, pullSize);
        if (CollectionUtils.isEmpty(headDatas)) {
            return new ArrayList<>();
        }
        List<Pair<Message, Double>> pairs = new ArrayList<>();
        for (ZSetOperations.TypedTuple<Object> headData : headDatas) {
            pairs.add(new Pair<>((Message) headData.getValue(), headData.getScore()));
        }
        return pairs;
    }
    
    /**
     * 删除指定key
     */
    public Boolean unlock(String key) {
        return redisClient.delete(key);
    }
    
    /**
     * 延时队列大小
     */
    public Long queueSize(String key) {
        return redisClient.zSize(key);
    }
    
    /**
     * 锁定指定key
     */
    public Boolean lock(String key, Duration duration) {
        return redisClient.setIfAbsent(key, "", duration);
    }
    
    /**
     * 发布重新平衡
     *
     * @param clientId 客户端id
     */
    public void publishRebalance(String clientId) {
        redisClient.convertAndSend(getRebalanceTopic(), clientId);
    }
    
    public Long executeLua(String lua, List<String> keys, Object... args) {
        return redisClient.executeLua(lua, keys, args);
    }
}
