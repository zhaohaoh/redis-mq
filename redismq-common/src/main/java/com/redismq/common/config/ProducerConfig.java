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
    public boolean ignoreRpcError = false;
    /**
     * 忽略rpc错误请求，只有同步的时候有效  20秒 rpc默认15秒
     */
    public long sendMaxTimeout = 20 * 1000;

}
