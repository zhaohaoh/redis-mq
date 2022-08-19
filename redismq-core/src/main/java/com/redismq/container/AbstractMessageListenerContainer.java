package com.redismq.container;

import com.google.common.collect.Sets;
import com.redismq.Message;
import com.redismq.core.RedisListenerEndpoint;
import com.redismq.core.RedisListenerRunnable;
import com.redismq.constant.QueueConstant;
import com.redismq.exception.RedisMqException;
import com.redismq.factory.DefaultRedisListenerContainerFactory;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.Queue;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.redismq.constant.QueueConstant.SPLITE;

/**
 * 容器抽象类
 */
public abstract class AbstractMessageListenerContainer {

    /**
     * 队列名
     */
    protected String queueName;

    /**
     * redis
     */
    protected RedisTemplate<String, Object> redisTemplate;
    /**
     * 最小线程数
     */
    protected int concurrency;
    /**
     * 最大线程数量
     */
    protected int maxConcurrency;
    /**
     * 重试最大次数
     */
    protected int retryMax;
    /**
     * 死信队列
     */
    protected String deadQueue;
    /**
     * ack模式
     */
    protected String ackMode;
    /**
     * 重试间隔
     */
    protected Integer retryInterval;
    /**
     * 端点map
     */
    protected Map<String, RedisListenerEndpoint> redisListenerEndpointMap;
    /**
     * 信号量
     */
    protected Semaphore semaphore;
    /**
     * 消费拦截器
     */
    protected List<ConsumeInterceptor> consumeInterceptorList;
    /**
     * 状态描述
     */
    protected volatile Integer state = RUNNING;
    private static final int RUNNING = 1;
    private static final int STOP = 1 << 1;
    private static final int BUSY = 1 << 2;
    private static final int PAUSE = 1 << 3;

    public void setConsumeInterceptorList(List<ConsumeInterceptor> consumeInterceptorList) {
        this.consumeInterceptorList = consumeInterceptorList;
    }

    public void setRedisListenerEndpointMap(Map<String, RedisListenerEndpoint> redisListenerEndpointMap) {
        this.redisListenerEndpointMap = redisListenerEndpointMap;
    }

    public AbstractMessageListenerContainer(DefaultRedisListenerContainerFactory redisListenerContainerFactory, Queue queue) {
        this.queueName = queue.getQueueName();
        this.redisTemplate = redisListenerContainerFactory.getRedisTemplate();
        this.concurrency = queue.getConcurrency();
        this.maxConcurrency = queue.getMaxConcurrency();
        this.retryMax = queue.getRetryMax();
        this.deadQueue = QueueConstant.DEFAULT_DEAD_QUEUE_PREFIX + queueName;
        this.ackMode = queue.getAckMode();
        this.retryInterval = redisListenerContainerFactory.getRetryInterval();
        this.semaphore = new Semaphore(queue.getMaxConcurrency());
    }

    public int pause() {
        return state = PAUSE;
    }


    public abstract void repush();

    public void stop() {
        this.state = STOP;
        doStop();
    }

    public abstract void doStop();

    public String getQueueName() {
        return queueName;
    }

    public String getDeadQueue() {
        return deadQueue;
    }

    public void setDeadQueue(String deadQueue) {
        this.deadQueue = deadQueue;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }


    public RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public int getRetryMax() {
        return retryMax;
    }

    public void setRetryMax(int retryMax) {
        this.retryMax = retryMax;
    }

    public Semaphore getSemaphore() {
        return semaphore;
    }

    public Integer getRetryInterval() {
        return retryInterval;
    }

    public boolean isStop() {
        return state == STOP;
    }
    public boolean isRunning() {
        return state == RUNNING;
    }

    public void running() {
        state = RUNNING;
    }

    public String getAckMode() {
        return ackMode;
    }

    protected RedisListenerRunnable createRedisListenerRunnable(String id, Object message) {
        RedisListenerEndpoint redisListenerEndpoint = redisListenerEndpointMap.get(id);
        if (redisListenerEndpoint == null) {
            throw new RedisMqException("not found RedisListenerEndpoint check your queue or tag");
        }
        RedisListenerRunnable runnable = new RedisListenerRunnable(redisListenerEndpoint.getBean(), redisListenerEndpoint.getMethod(),
                this.getRetryMax(), this.getSemaphore(),
                this.getRedisTemplate());
        runnable.setArgs(message);
        runnable.setAckMode(this.getAckMode());
        runnable.setRetryInterval(this.getRetryInterval());
        runnable.setQueueName(queueName);
        runnable.setConsumeInterceptors(consumeInterceptorList);
        return runnable;
    }

    protected String getRunableKey(String tag) {
        return queueName + SPLITE + tag;
    }

}