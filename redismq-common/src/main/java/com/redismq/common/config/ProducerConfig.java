package com.redismq.common.config;


import com.redismq.common.constant.ProducerAck;
import lombok.Data;


@Data
public class ProducerConfig {
    /**
     * 生产者重试次数
     */
    public int producerRetryCount = 30;
    /**
     * 生产 loadBalance 加载服务 重试
     */
    public int loadBalanceRetryCount  = 100;
    /**
     * 生产 loadBalance 加载服务 重试间隔
     */
    public int loadBalanceRetryMills = 10;
    /**
     * 生产者重试间隔
     */
    public int producerRetrySleep = 200;
    /**
     * 生产者批量发送消息限制最大数量
     */
    public int producerMaxBatchSize = 200;
    /**
     * 生产者消息确认机制
     */
    public ProducerAck productAck = ProducerAck.ASYNC;
    /**
     * 打印核心生产日志
     */
    public boolean printProducerLog = true;
    /**
     * 忽略rpc错误请求，只有同步的时候有效
     */
    public boolean ignoreRpcError = true;
    /**
     *  发送消息超时60秒 rpc默认30秒
     */
    public long sendMaxTimeout = 60 * 1000;
    /**
     * 生产者异步发送队列大小。小了影响吞吐量，默认10000
     */
    public int producerBasketSize = 10000;

}
