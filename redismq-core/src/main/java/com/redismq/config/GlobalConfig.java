package com.redismq.config;


import lombok.Data;


@Data
public class GlobalConfig {

    /**
     * 延时队列拉取的头部消息数量
     */
    public int delayQueuePullSize = 100;
    /**
     * 单个虚拟队列消费的锁定时间 有看门狗 如果有服务下线了  会锁定这个时间内其他服务不能消费
     */
    public int virtualLockTime = 11;
    /**
     * 单个虚拟队列消费看门狗的续期时间
     */
    public int virtualLockWatchDogTime = 5;
    /**
     * 生产者重试次数
     */
    public int producerRetryCount = 3;
    /**
     * 生产者重试间隔
     */
    public int producerRetrySleep = 200;
    /**
     * 事务提交后发送
     */
    public boolean sendAfterCommit = true;
    /**
     * 开启seata事务
     */
    public boolean seataState = false;

    /**
     * 打印核心消费日志
     */
    public boolean printConsumeLog = false;
}
