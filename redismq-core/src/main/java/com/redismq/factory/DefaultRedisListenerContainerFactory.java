package com.redismq.factory;


import com.redismq.connection.RedisClient;
import com.redismq.container.AbstractMessageListenerContainer;
import com.redismq.container.RedisMQListenerContainer;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.Queue;
import lombok.Data;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.List;
@Data
public class DefaultRedisListenerContainerFactory
        implements RedisListenerContainerFactory {
    /**
     * 相关的一些配置
     */
    private RedisClient redisClient;
    private int concurrency;
    private int maxConcurrency;
    private int retryMax;
    //ack模式
    private String ackMode;
    //重试次数
    private int retryInterval;
    private Duration timeout;
    /**
     * 消费拦截器
     */
    private List<ConsumeInterceptor> consumeInterceptors;

    @Override
    public AbstractMessageListenerContainer createListenerContainer(Queue queue) {
        AbstractMessageListenerContainer instance;
        instance = new RedisMQListenerContainer(this, queue);
        instance.setConsumeInterceptorList(consumeInterceptors);
        return instance;
    }
}