package com.redismq.container;

import com.redismq.connection.RedisClient;
import com.redismq.core.RedisListenerCallable;
import com.redismq.core.RedisListenerEndpoint;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.Queue;
import lombok.Data;

import java.util.List;
import java.util.Map;

import static com.redismq.constant.GlobalConstant.SPLITE;
import static com.redismq.constant.StateConstant.*;

/**
 * 容器抽象类
 */
@Data
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

    public AbstractMessageListenerContainer(RedisClient redisClient, Queue queue,List<ConsumeInterceptor> consumeInterceptorList) {
        this.queueName = queue.getQueueName();
        this.redisClient = redisClient;
        this.concurrency = queue.getConcurrency();
        this.maxConcurrency = queue.getMaxConcurrency();
        this.delay = queue.isDelayState();
        this.retryMax = queue.getRetryMax();
        this.ackMode = queue.getAckMode();
        this.retryInterval = queue.getRetryInterval();
        this.consumeInterceptorList =  consumeInterceptorList;
    }

    /**
     * 当前容器暂停拉取消息
     */
    public int pause() {
        return state = PAUSE;
    }
    /**
     * 当前容器是否暂停了
     */
    public boolean isPause() {
        return state == PAUSE;
    }

    public abstract void repush();
    /**
     * 当前容器是否停止了
     */
    public void stop() {
        this.state = STOP;
        doStop();
    }

    public abstract void doStop();

    public boolean isRunning() {
        return state == RUNNING;
    }

    public void running() {
        if (state != RUNNING) {
            state = RUNNING;
        }
    }

    protected RedisListenerCallable getRedisListenerCallable(String id, Object message) {
        RedisListenerEndpoint redisListenerEndpoint = redisListenerEndpointMap.get(id);
        if (redisListenerEndpoint == null) {
            throw new RedisMqException("not found RedisListenerEndpoint check your queue or tag");
        }
        RedisListenerCallable runnable = new RedisListenerCallable(redisListenerEndpoint.getBean(), redisListenerEndpoint.getMethod(),
                this.getRetryMax(),
                this.redisClient);
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