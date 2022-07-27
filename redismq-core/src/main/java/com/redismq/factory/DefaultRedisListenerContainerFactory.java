package com.redismq.factory;


import com.redismq.container.AbstractMessageListenerContainer;
import com.redismq.container.RedisMQListenerContainer;
import com.redismq.queue.Queue;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;

public class DefaultRedisListenerContainerFactory
        implements RedisListenerContainerFactory {
    /**
     * 相关的一些配置
     */
    private RedisTemplate<String, Object> redisTemplate;
    private int concurrency;
    private int maxConcurrency;
    private int retryMax;
    //ack模式
    private String ackMode;
    //重试次数
    private int retryInterval;
    private Duration timeout;

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }


    public RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
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


    @Override
    public AbstractMessageListenerContainer createListenerContainer(Queue queue) {
        AbstractMessageListenerContainer instance;
        instance = new RedisMQListenerContainer(this, queue);
        return instance;
    }
}