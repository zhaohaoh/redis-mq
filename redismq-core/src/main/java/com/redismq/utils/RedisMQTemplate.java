package com.redismq.utils;

import com.redismq.common.pojo.Message;
import com.redismq.core.RedisMQProducer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * redisMQ操作模板
 *
 * @author hzh
 * @date 2022/12/13
 */
public class RedisMQTemplate {
    private final RedisMQProducer redisMQProducer;

    public RedisMQTemplate(RedisMQProducer redisMQProducer) {
        this.redisMQProducer = redisMQProducer;
    }

    public RedisMQProducer getRedisMQProducer() {
        return redisMQProducer;
    }

    /**
     * 队列消息
     */
    public boolean sendMessage(Message message) {
        return redisMQProducer.sendMessage(message);
    }

    /**
     * 发送延时消息
     */
    public boolean sendDelayMessage(Message message, Long delayTime, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(delayTime);
        return redisMQProducer.sendDelayMessage(message, millis);
    }

    /**
     * 发送延时消息
     *
     */
    public boolean sendDelayMessage(Message message, Duration duration) {
        long millis = duration.toMillis();
        return redisMQProducer.sendDelayMessage(message, millis);
    }

    /**
     * 发送定时消息
     *
     */
    public boolean sendTimingMessage(Message message, Long excuteTime) {
        return redisMQProducer.sendTimingMessage(message, excuteTime);
    }

    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queue) {
        return redisMQProducer.sendMessage(obj, queue);
    }

    /**
     * 带tag的队列消息
     */
    public boolean sendMessage(Object obj, String queue, String tag) {
        return redisMQProducer.sendMessage(obj, queue, tag,"");
    }
    
    /**
     * 带tag和key的队列消息
     */
    public boolean sendMessage(Object obj, String queue, String tag,String key) {
        return redisMQProducer.sendMessage(obj, queue, tag,key);
    }

    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费
     */
    public boolean sendBatchMessage(List<?> messages, String queue) {
        return redisMQProducer.sendBatchMessage(messages, queue, "");
    }

    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费
     */
    public boolean sendBatchMessage(List<?> messages, String queue, String tag) {
        return redisMQProducer.sendBatchMessage(messages, queue, tag);
    }


    /**
     * 带tag的队列延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, String tag, Long delayTime, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(delayTime);
        return redisMQProducer.sendDelayMessage(obj, queue, tag,"", millis);
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, Long delayTime, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(delayTime);
        return redisMQProducer.sendDelayMessage(obj, queue, "","", millis);
    }

    /**
     * 带tag的队列延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, String tag, Duration duration) {
        long millis = duration.toMillis();
        return redisMQProducer.sendDelayMessage(obj, queue, tag,"", millis);
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, Duration duration) {
        long millis = duration.toMillis();
        return redisMQProducer.sendDelayMessage(obj, queue, "","", millis);
    }

    /**
     * 定时消息
     */
    public boolean sendTimingMessage(Object obj, String queue, String tag, Long executorTime) {
        return redisMQProducer.sendTimingMessage(obj, queue, tag,"", executorTime);
    }

    public boolean sendTimingMessage(Object obj, String queue, Long executorTime) {
        return redisMQProducer.sendTimingMessage(obj, queue, "","", executorTime);
    }
   

    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisMQProducer.publish(topic, obj);
    }

    /*
     * 尝试取消删除消息-注意消息有可能已经被消费
     */
    public void tryCancel(String queueName,String msgId) {
        redisMQProducer.tryCancel(queueName,msgId);
    }
}
