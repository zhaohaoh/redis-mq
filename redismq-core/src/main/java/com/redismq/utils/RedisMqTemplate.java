package com.redismq.utils;

import com.redismq.Message;
import com.redismq.core.RedisMQProducer;
import com.redismq.exception.RedisMqException;
import com.redismq.queue.QueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RedisMqTemplate {
    @Autowired
    private RedisMQProducer redisMQProducer;

    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queue) {
        redisMQProducer.sendMessage(obj, queue);
        return true;
    }

    /**
     * 带tag的队列消息
     */
    public boolean sendMessage(Object obj, String queue, String tag) {
        hasQueue(queue);
        Message message = new Message();
        message.setContent(obj);
        message.setTag(tag);
        return redisMQProducer.sendMessage(obj, queue, tag);
    }

    public boolean sendDelayMessage(Object obj, String queue, String tag, Integer delayTime) {
        redisMQProducer.sendDelayMessage(obj, queue, tag, delayTime);
        return true;
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queue, Integer delayTime) {
        redisMQProducer.sendDelayMessage(obj, queue, "", delayTime);
        return true;
    }

    /**
     * 定时消息
     */
    public boolean sendTimingMessage(Object obj, String queue,String tag, Long executorTime) {
        redisMQProducer.sendTimingMessage(obj, queue, tag,executorTime);
        return true;
    }

    public boolean sendTimingMessage(Object obj, String queue, Long executorTime) {
        redisMQProducer.sendTimingMessage(obj, queue, "",executorTime);
        return true;
    }

    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisMQProducer.publish(topic, obj);
    }

    private void hasQueue(String name) {
        boolean has = QueueManager.matchQueue(name);
        if (!has) {
            throw new RedisMqException("redismq  not found queue");
        }
    }


}
