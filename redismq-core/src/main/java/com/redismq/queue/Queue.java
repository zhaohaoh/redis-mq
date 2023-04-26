package com.redismq.queue;

import lombok.Data;

/**
 * @author hzh
 * @date 2021/8/12
 * redis队列    默认值都为空。如果不为空说明被注解或者配置填充了属性则替换DefaultRedisListenerContainerFactory中的默认值
 */
@Data
public class Queue {
    /**
     * 队列名称
     */
    private String queueName;
    /**
     * 消费失败重试次数
     */
    private Integer retryMax;
    /**
     * ack模式
     */
    private String ackMode;
    /**
     * 消费者 此处得消费对应得是每个队列得消费者。假如有5个队列。就会有5个线程池。分别最小消费者是1. 会被注解redis监听替换
     */
    private Integer concurrency;
    /**
     * 最大消费者
     */
    private Integer maxConcurrency;
    /**
     * 是否延时队列
     */
    private boolean delayState = false;
    /**
     * 虚拟队列数量
     */
    private Integer virtual ;

    /**
     * 虚拟队列数量
     */
    private Integer queueMaxSize;
}
