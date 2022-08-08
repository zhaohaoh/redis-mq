package com.redismq.core;

import com.redismq.Message;
import com.redismq.constant.RedisMQConstant;
import com.redismq.constant.PushMessage;
import com.redismq.exception.RedisMqException;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.redismq.constant.QueueConstant.SPLITE;
import static com.redismq.constant.RedisMQConstant.REDIS_MQ_SEND_MSG_INCREMENT;

/**
 * @Author: hzh
 * @Date: 2022/5/19 15:46
 * redismq生产者
 */
public class RedisMQProducer {
    protected final Logger log = LoggerFactory.getLogger(RedisMQProducer.class);
    private final RedisTemplate<String, Object> redisTemplate;
    private Integer retryCount = 3;
    private Integer retrySleep = 200;

    public RedisMQProducer(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public void setRetrySleep(Integer retrySleep) {
        this.retrySleep = retrySleep;
    }


    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queueName) {
        Queue queue = hasQueue(queueName);
        Message message = new Message();
        message.setContent(obj);
        return sendMessage(queue, message, null);
    }

    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queueName, String tag) {
        Queue queue = hasQueue(queueName);
        Message message = new Message();
        message.setContent(obj);
        message.setTag(tag);
        return sendMessage(queue, message, null);
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queueName, String tag, Integer delayTime) {
        Queue queue = hasDelayQueue(queueName);
        Message message = new Message();
        message.setContent(obj);
        message.setTag(tag);
        long executorTime = System.currentTimeMillis() + (delayTime * 1000);
        return RedisMQProducer.this.sendMessage(queue, message, executorTime);
    }

    /**
     * 发送定时消息
     */
    public boolean sendTimingMessage(Object obj, String queueName, String tag, Long executorTime) {
        Queue queue = hasDelayQueue(queueName);
        Message message = new Message();
        message.setContent(obj);
        message.setTag(tag);
        return RedisMQProducer.this.sendMessage(queue, message, executorTime);
    }

    private boolean sendMessage(Queue queue, Message message, Long executorTime) {
        try {
            Long increment = redisTemplate.opsForValue().increment(REDIS_MQ_SEND_MSG_INCREMENT);
            increment = increment == null ? 0 : increment;
            //此处bug 多机无法保证顺序
            if (executorTime == null) {
                executorTime = increment;
            }
            long num = increment % QueueManager.VIRTUAL_QUEUES_NUM;
            PushMessage pushMessage = new PushMessage();
            pushMessage.setTimestamp(executorTime);
            pushMessage.setQueue(queue.getQueueName() + SPLITE + num);
            String lua = "local size = redis.call('zcard', KEYS[1]);\n" +
                    "if size and tonumber(size) >=" + queue.getQueueMaxSize() + " then  \n" +
                    "return -1;\n" +
                    "end\n" +
                    "redis.call('zadd', KEYS[1], ARGV[3], ARGV[2]);\n" +
                    "redis.call('publish', KEYS[2], ARGV[1]);\n" +
                    "return size";
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
            List<String> list = new ArrayList<>();
            list.add(queue.getQueueName() + SPLITE + num);
            list.add(RedisMQConstant.TOPIC);
            message.setQueueName(queue.getQueueName() + SPLITE + num);
            Long size = -1L;
            int count = 0;
            // 发送的重试 如果redis中数量超过1万也会重试.
            while (size == null || count < retryCount) {
                size = redisTemplate.execute(redisScript, list, pushMessage, message, executorTime);
                if (size == null || size < 0) {
                    try {
                        Thread.sleep(retrySleep);
                    } catch (InterruptedException ignored) {
                    }
                    count++;
                    log.warn("RedisMQ sendMessage retry");
                } else {
                    log.info("RedisMQ sendMessage success");
                    return true;
                }
            }
            log.error("RedisMQ Producer Queue Full");
            return false;
        } catch (Exception e) {
            log.error("RedisMQ Send Message Fail", e);
            return false;
        }
    }


    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisTemplate.convertAndSend(topic, obj);
    }


    private Queue hasQueue(String name) {
        Queue queue = QueueManager.getQueue(name);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (queue.getDelayState()) {
            throw new RedisMqException("Redismq Queue type mismatch");
        }
        return queue;
    }

    private Queue hasDelayQueue(String name) {
        Queue queue = QueueManager.getQueue(name);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (!queue.getDelayState()) {
            throw new RedisMqException("Redismq Queue type mismatch");
        }
        return queue;
    }

}
