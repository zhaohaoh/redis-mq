package com.redismq.core;

import com.redismq.Message;
import com.redismq.constant.RedisMQConstant;
import com.redismq.constant.PushMessage;
import com.redismq.exception.QueueFullException;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.pojo.SendMessageParam;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        return sendMessage(obj, topic, "");
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
        return sendSingleMessage(queue, message, null);
    }

    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费
     */
    public boolean sendBatchMessage(List<?> objs, String topic) {
        return sendBatchMessage(objs, topic, "");
    }

    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费
     */
    public boolean sendBatchMessage(List<?> objs, String topic, String tag) {
        Queue queue = hasQueue(topic);
        List<Message> messages = new ArrayList<>();
        for (Object obj : objs) {
            Message message = new Message();
            message.setTopic(topic);
            message.setBody(obj);
            message.setTag(tag);
            messages.add(message);
        }
        return sendBatchMessage(queue, messages);
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
        return RedisMQProducer.this.sendSingleMessage(queue, message, executorTime);
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
        return RedisMQProducer.this.sendSingleMessage(queue, message, executorTime);
    }

    public boolean sendSingleMessage(Queue queue, Message message, Long executorTime) {
        Long increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
        increment = increment == null ? 0 : increment;
        if (increment >= System.currentTimeMillis()) {
            redisTemplate.opsForValue().set(RedisMQConstant.getSendIncrement(), 0L);
            increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
            increment = increment == null ? 0 : increment;
        }
        if (executorTime == null) {
            executorTime = increment;
        }
        long num = increment % QueueManager.VIRTUAL_QUEUES_NUM;
        PushMessage pushMessage = new PushMessage();
        pushMessage.setTimestamp(0L);
        String virtualQueue = queue.getQueueName() + SPLITE + num;
        pushMessage.setQueue(virtualQueue);
        message.setVirtualQueueName(virtualQueue);
        SendMessageParam sendMessageParam = new SendMessageParam();
        sendMessageParam.setMessage(message);
        sendMessageParam.setExecutorTime(executorTime);
        return doSendMessage(pushMessage, Collections.singletonList(sendMessageParam));
    }

    public boolean sendBatchMessage(Queue queue, List<Message> messages) {
        Map<PushMessage, List<SendMessageParam>> messageMap = new HashMap<>();
        for (Message message : messages) {
            //有bug要改成lua
            Long increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
            increment = increment == null ? 0 : increment;
            if (increment >= System.currentTimeMillis()) {
                redisTemplate.opsForValue().set(RedisMQConstant.getSendIncrement(), 0L);
                increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
                increment = increment == null ? 0 : increment;
            }
            Long executorTime = increment;
            long num = increment % QueueManager.VIRTUAL_QUEUES_NUM;
            PushMessage pushMessage = new PushMessage();
            pushMessage.setTimestamp(0L);
            String virtualQueue = queue.getQueueName() + SPLITE + num;
            pushMessage.setQueue(virtualQueue);
            message.setVirtualQueueName(virtualQueue);
            SendMessageParam sendMessageParam = new SendMessageParam();
            sendMessageParam.setMessage(message);
            sendMessageParam.setExecutorTime(executorTime);
            List<SendMessageParam> messageList = messageMap.computeIfAbsent(pushMessage, m -> new ArrayList<>());
            messageList.add(sendMessageParam);
        }
        // 批次发送有一次失败即为失败
        AtomicBoolean result = new AtomicBoolean(true);
        messageMap.forEach((pushMessage, sendMessageParams) -> {
            boolean success = doSendMessage(pushMessage, sendMessageParams);
            if (!success) {
                result.set(false);
            }
        });
        return result.get();
    }

    private boolean doSendMessage(PushMessage pushMessage, List<SendMessageParam> sendMessageParams) {
        //构建redis的请求参数按顺序消息和scope对应
        List<Message> messageList = sendMessageParams.stream().map(SendMessageParam::getMessage).collect(Collectors.toList());
        try {
            List<Object> params = new ArrayList<>();
            beforeSend(messageList);
            for (SendMessageParam message : sendMessageParams) {
                params.add(message.getMessage());
                params.add(message.getExecutorTime());
            }
            boolean success = this.sendRedisMessage(pushMessage, params);
            if (success) {
                afterSend(messageList);
            } else {
                onFail(messageList, new QueueFullException("RedisMQ Producer Queue Full"));
            }
            return success;
        } catch (Exception e) {
            onFail(messageList, e);
            log.error("RedisMQProducer doSendMessage Exception:",e);
            return false;
        }
    }

    private boolean sendRedisMessage(PushMessage pushMessage, List<?> params) {
        Queue queue = QueueManager.getQueueByVirtual(pushMessage.getQueue());
        StringBuilder sb = new StringBuilder();
        sb.append("local size = redis.call('zcard', KEYS[1]);\n" +
                "if size and tonumber(size) >=" + queue.getQueueMaxSize() + " then  \n" +
                "return -1;\n" +
                "end\n");
        sb.append("redis.call('zadd', KEYS[1]");
        for (int i = 0; i < params.size() >> 1; i++) {
            sb.append(", ARGV[" + (i + 3) + "], ARGV[" + (i + 2) + "]");
        }
        sb.append(");\n");
        sb.append("redis.call('publish', KEYS[2], ARGV[1]);\n" +
                "return size");
        String lua = sb.toString();
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
        List<String> list = new ArrayList<>();
        list.add(pushMessage.getQueue());
        list.add(RedisMQConstant.getTopic());
        Long size = -2L;
        Object[] array = new Object[params.size() + 1];
        array[0] = pushMessage;
        for (int i = 0; i < params.size(); i++) {
            array[i + 1] = params.get(i);
        }
        int count = 0;
        while (size == null || count < retryCount) {
            size = redisTemplate.execute(redisScript, list, array);
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
        if (size == -1L) {
            log.error("RedisMQ Producer Queue Full");
            return false;
        }
        return false;
    }


//    private boolean sendMessage(Queue queue, Message message, Long executorTime) {
//        try {
//            Long increment = redisTemplate.opsForValue().increment(RedisMQConstant.getSendIncrement());
//            increment = increment == null ? 0 : increment;
//            if (increment >= Long.MAX_VALUE) {
//                redisTemplate.opsForValue().set(RedisMQConstant.getSendIncrement(), 0L);
//            }
//            long num = increment % QueueManager.VIRTUAL_QUEUES_NUM;
//            PushMessage pushMessage = new PushMessage();
//            pushMessage.setQueue(queue.getQueueName() + SPLITE + num);
//            //普通消息不需要设置执行时间.自动取当前时间
//            if (executorTime == null) {
//                executorTime = increment;
//            } else {
//                pushMessage.setTimestamp(executorTime);
//            }
//
//            String lua = "local size = redis.call('zcard', KEYS[1]);\n" +
//                    "if size and tonumber(size) >=" + queue.getQueueMaxSize() + " then  \n" +
//                    "return -1;\n" +
//                    "end\n" +
//                    "redis.call('zadd', KEYS[1], ARGV[3], ARGV[2]);\n" +
//                    "redis.call('publish', KEYS[2], ARGV[1]);\n" +
//                    "return size";
//            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(lua, Long.class);
//            List<String> list = new ArrayList<>();
//            list.add(queue.getQueueName() + SPLITE + num);
//            list.add(RedisMQConstant.getTopic());
//            message.setVirtualQueueName(queue.getQueueName() + SPLITE + num);
//            Long size = -2L;
//            int count = 0;
//            beforeSend(Collections.singletonList(message));
//            // 发送的重试 如果redis中数量超过1万也会重试.
//            while (size == null || count < retryCount) {
//                size = redisTemplate.execute(redisScript, list, pushMessage, message, executorTime);
//                if (size == null || size < 0) {
//                    try {
//                        Thread.sleep(retrySleep);
//                    } catch (InterruptedException ignored) {
//                    }
//                    count++;
//                    log.warn("RedisMQ sendMessage retry");
//                } else {
//                    log.info("RedisMQ sendMessage success");
//                    afterSend(Collections.singletonList(message));
//                    return true;
//                }
//            }
//            if (size == -1L) {
//                log.error("RedisMQ Producer Queue Full");
//                onFail(Collections.singletonList(message), new QueueFullException("RedisMQ Producer Queue Full"));
//            } else {
//                onFail(Collections.singletonList(message), null);
//            }
//            return false;
//        } catch (Exception e) {
//            log.error("RedisMQ Send Message Fail", e);
//            onFail(Collections.singletonList(message), e);
//            return false;
//        }
//    }

    private void afterSend(List<Message> message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                interceptor.afterSend(message);
            }
        }
    }

    private void onFail(List<Message> message, Exception e) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                interceptor.onFail(message, e);
            }
        }
    }

    private void beforeSend(List<Message> message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            for (ProducerInterceptor interceptor : producerInterceptors) {
                interceptor.beforeSend(message);
            }
        }
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
