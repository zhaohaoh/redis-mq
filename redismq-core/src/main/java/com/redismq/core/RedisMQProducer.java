package com.redismq.core;

import com.redismq.Message;
import com.redismq.constant.RedisMQConstant;
import com.redismq.constant.PushMessage;
import com.redismq.exception.QueueFullException;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static com.redismq.constant.QueueConstant.SPLITE;

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
    private List<ProducerInterceptor> producerInterceptors;

    public RedisMQProducer(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public void setRetrySleep(Integer retrySleep) {
        this.retrySleep = retrySleep;
    }

    public void setProducerInterceptors(List<ProducerInterceptor> producerInterceptors) {
        this.producerInterceptors = producerInterceptors;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public Integer getRetrySleep() {
        return retrySleep;
    }

    public List<ProducerInterceptor> getProducerInterceptors() {
        return producerInterceptors;
    }


    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String topic) {
        Queue queue = hasQueue(topic);
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(obj);
        return sendMessage(queue, message, null);
    }

    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String topic, String tag) {
        Queue queue = hasQueue(topic);
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(obj);
        message.setTag(tag);
        return sendMessage(queue, message, null);
    }

    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String topic, String tag, Integer delayTime) {
        Queue queue = hasDelayQueue(topic);
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(obj);
        message.setTag(tag);
        long executorTime = System.currentTimeMillis() + (delayTime * 1000);
        return RedisMQProducer.this.sendMessage(queue, message, executorTime);
    }

    /**
     * 发送定时消息
     */
    public boolean sendTimingMessage(Object obj, String topic, String tag, Long executorTime) {
        Queue queue = hasDelayQueue(topic);
        Message message = new Message();
        message.setTopic(topic);
        message.setBody(obj);
        message.setTag(tag);
        return RedisMQProducer.this.sendMessage(queue, message, executorTime);
    }

    private boolean sendMessage(Queue queue, Message message, Long executorTime) {
        try {
            Long increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
            increment = increment == null ? 0 : increment;
            if (increment >= Long.MAX_VALUE) {
                redisTemplate.opsForValue().set(RedisMQConstant.getSendIncrement(), 0L);
            }
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
            list.add(RedisMQConstant.getTopic());
            message.setVirtualQueueName(queue.getQueueName() + SPLITE + num);
            Long size = -2L;
            int count = 0;
            beforeSend(message);
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
                    afterSend(message);
                    return true;
                }
            }
            if (size == -1L) {
                log.error("RedisMQ Producer Queue Full");
                onFail(message, new QueueFullException("RedisMQ Producer Queue Full"));
            } else {
                onFail(message, null);
            }
            return false;
        } catch (Exception e) {
            log.error("RedisMQ Send Message Fail", e);
            onFail(message, e);
            return false;
        }
    }

    private void afterSend(Message message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                interceptor.afterSend(message);
            }
        }
    }

    private void onFail(Message message, Exception e) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                interceptor.onFail(message, e);
            }
        }
    }

    private Message beforeSend(Message message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                 interceptor.beforeSend(message);
            }
        }
        return message;
    }

    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisTemplate.convertAndSend(topic, obj);
    }


    private Queue hasQueue(String topic) {
        String queueName = RedisMQConstant.getQueueNameByTopic(topic);
        Queue queue = QueueManager.getQueue(queueName);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (queue.getDelayState()) {
            throw new RedisMqException("Redismq Queue type mismatch");
        }
        return queue;
    }

    private Queue hasDelayQueue(String topic) {
        String queueName = RedisMQConstant.getQueueNameByTopic(topic);
        Queue queue = QueueManager.getQueue(queueName);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (!queue.getDelayState()) {
            throw new RedisMqException("Redismq DelayQueue type mismatch");
        }
        return queue;
    }

}
