package com.redismq.autoconfigure;

import com.redismq.config.GlobalConfig;
import com.redismq.config.RedisProperties;
import com.redismq.constant.AckMode;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @Author: hzh
 * @Date: 2022/11/4 15:29
 */
@Data
@ConfigurationProperties(prefix = RedisMQProperties.PREFIX)
public class RedisMQProperties {
    public static final String PREFIX = "spring.redismq";
    /**
     * 消费者 此处得消费对应得是每个队列得消费者。假如有5个队列。就会有5个线程池。分别最小消费者是1. 会被注解redis监听替换
     */
    private int concurrency = 1;
    /**
     * 最大消费者
     */
    private int maxConcurrency = 64;
    /**
     * 消费者重试次数
     */
    private int consumeRetryMax = 1;
    /**
     * 生产者重试次数
     */
    private int producerRetryMax = 1;
    /**
     * 发送者重试时间间隔
     */
    private int producerRetryInterval = 200;
    /**
     * 消费者重试时间间隔
     */
    private int retryInterval = 500;
    /**
     * ack模式
     */
    private String ackMode = AckMode.AUTO;
    /**
     * 虚拟队列数量
     */
    private Integer virtual = 1;

    /**
     * 环境隔离统一后缀
     */
    private String group = "";

    /**
     * redis属性
     */
    private RedisProperties redisProperties;


    @NestedConfigurationProperty
    private GlobalConfig globalConfig = new GlobalConfig();
}
