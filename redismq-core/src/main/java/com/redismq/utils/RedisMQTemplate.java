package com.redismq.utils;

import com.redismq.Message;
import com.redismq.core.RedisMQProducer;
import com.redismq.exception.RedisMqException;
import com.redismq.queue.QueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    public boolean sendMessage(Object obj, String queue) {
        return redisMQProducer.sendMessage(obj, queue);
    }

    /**
     * 带tag的队列消息
     */
    public boolean sendMessage(Object obj, String queue, String tag) {
        return redisMQProducer.sendMessage(obj, queue, tag);
    }

    /**
     * 带tag的队列延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, String tag, Integer delayTime) {
        return redisMQProducer.sendDelayMessage(obj, queue, tag, delayTime);
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, Integer delayTime) {
        return redisMQProducer.sendDelayMessage(obj, queue, "", delayTime);
    }

    /**
     * 定时消息
     */
    public boolean sendTimingMessage(Object obj, String queue, String tag, Long executorTime) {
        return redisMQProducer.sendTimingMessage(obj, queue, tag, executorTime);

    }

    public boolean sendTimingMessage(Object obj, String queue, Long executorTime) {
        return redisMQProducer.sendTimingMessage(obj, queue, "", executorTime);

    }

    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisMQProducer.publish(topic, obj);
    }


}
