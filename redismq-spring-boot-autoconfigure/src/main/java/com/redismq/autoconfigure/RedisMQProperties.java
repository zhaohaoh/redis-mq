package com.redismq.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

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
    private int maxConcurrency = 1;
    /**
     * 消费者重试次数
     */
    private int consumeRetryMax = 1;
    /**
     * 生产者重试次数
     */
    private int producerRetryMax = 1;
    /**
     * 发送者重试时间
     */
    private int producerRetryInterval = 200;
    /**
     * 消费者重试时间
     */
    private int retryInterval = 500;
    /**
     * ack模式
     */
    private String ackMode = "auto";
    /**
     * 虚拟队列数量
     */
    private Integer virtual = 1;

    /**
     * 环境隔离统一后缀
     */
    private String group = "";

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getVirtual() {
        return virtual;
    }

    public void setVirtual(Integer virtual) {
        this.virtual = virtual;
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

    public int getConsumeRetryMax() {
        return consumeRetryMax;
    }

    public void setConsumeRetryMax(int consumeRetryMax) {
        this.consumeRetryMax = consumeRetryMax;
    }

    public int getProducerRetryMax() {
        return producerRetryMax;
    }

    public void setProducerRetryMax(int producerRetryMax) {
        this.producerRetryMax = producerRetryMax;
    }

    public int getProducerRetryInterval() {
        return producerRetryInterval;
    }

    public void setProducerRetryInterval(int producerRetryInterval) {
        this.producerRetryInterval = producerRetryInterval;
    }
}
