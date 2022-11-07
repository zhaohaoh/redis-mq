package com.redismq.container;

import com.redismq.Message;
import com.redismq.connection.RedisClient;
import com.redismq.core.RedisListenerEndpoint;
import com.redismq.core.RedisListenerRunnable;
import com.redismq.exception.RedisMqException;
import com.redismq.factory.DefaultRedisListenerContainerFactory;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.Queue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static com.redismq.constant.GlobalConstant.SPLITE;
import static com.redismq.constant.StateConstant.*;

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
    protected RedisClient redisClient;
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
    protected Boolean delay;
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

    protected final Map<String, Message> localMessages = new ConcurrentHashMap<>();
    /**
     * 消费拦截器
     */
    protected List<ConsumeInterceptor> consumeInterceptorList;
    /**
     * 状态描述
     */
    protected volatile int state = RUNNING;


    public void setConsumeInterceptorList(List<ConsumeInterceptor> consumeInterceptorList) {
        this.consumeInterceptorList = consumeInterceptorList;
    }

    public void setRedisListenerEndpointMap(Map<String, RedisListenerEndpoint> redisListenerEndpointMap) {
        this.redisListenerEndpointMap = redisListenerEndpointMap;
    }

    public AbstractMessageListenerContainer(DefaultRedisListenerContainerFactory redisListenerContainerFactory, Queue queue) {
        this.queueName = queue.getQueueName();
        this.redisClient = redisListenerContainerFactory.getRedisClient();
        this.concurrency = queue.getConcurrency();
        this.maxConcurrency = queue.getMaxConcurrency();
        this.delay = queue.getDelayState();
        this.retryMax = queue.getRetryMax();
        this.ackMode = queue.getAckMode();
        this.retryInterval = redisListenerContainerFactory.getRetryInterval();
        this.semaphore = new Semaphore(queue.getMaxConcurrency());
    }

    public int pause() {
        return state = PAUSE;
    }

    public boolean isPause() {
        return state == PAUSE;
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


    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public RedisClient redisClient() {
        return redisClient;
    }

    public void setRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
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

    public boolean isRunning() {
        return state == RUNNING;
    }

    public void running() {
        if (state != RUNNING) {
            state = RUNNING;
        }
    }

    public String getAckMode() {
        return ackMode;
    }

    protected RedisListenerRunnable getRedisListenerRunnable(String id, Object message) {
        RedisListenerEndpoint redisListenerEndpoint = redisListenerEndpointMap.get(id);
        if (redisListenerEndpoint == null) {
            throw new RedisMqException("not found RedisListenerEndpoint check your queue or tag");
        }
        RedisListenerRunnable runnable = new RedisListenerRunnable(redisListenerEndpoint.getBean(), redisListenerEndpoint.getMethod(),
                this.getRetryMax(), this.getSemaphore(),
                this.redisClient);
        runnable.setArgs(message);
        runnable.setAckMode(this.getAckMode());
        runnable.setRetryInterval(this.getRetryInterval());
        runnable.setQueueName(queueName);
        runnable.setConsumeInterceptors(consumeInterceptorList);
        runnable.setLocalMessages(localMessages);
        return runnable;
    }

    protected String getRunableKey(String tag) {
        return queueName + SPLITE + tag;
    }

}