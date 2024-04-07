package com.redismq.core;

import com.google.common.collect.Lists;
import com.redismq.Message;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.PushMessage;
import com.redismq.constant.RedisMQConstant;
import com.redismq.exception.QueueFullException;
import com.redismq.exception.RedisMqException;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.pojo.SendMessageParam;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.utils.RedisMQDataHelper;
import com.redismq.utils.RedisMQStringMapper;
import com.redismq.utils.SeataUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.seata.core.context.RootContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.redismq.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.constant.GlobalConstant.V_QUEUE_SPLITE;

/**
 * @Author: hzh
 * @Date: 2022/5/19 15:46 redismq生产者
 */
public class RedisMQProducer {
    
    protected final Logger log = LoggerFactory.getLogger(RedisMQProducer.class);
    
    private RedisMQClientUtil redisMQClientUtil;
    
    private List<ProducerInterceptor> producerInterceptors;
    
    private SeataUtil seataUtil;
    
    public List<ProducerInterceptor> getProducerInterceptors() {
        return producerInterceptors;
    }
    
    public void setProducerInterceptors(List<ProducerInterceptor> producerInterceptors) {
        this.producerInterceptors = producerInterceptors;
    }
    
    
    public RedisMQProducer(RedisMQClientUtil redisMQClientUtil) {
        this.redisMQClientUtil = redisMQClientUtil;
        if (GLOBAL_CONFIG.seataState) {
            this.seataUtil = new SeataUtil();
        }
    }
    
    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queue, String key) {
        return sendMessage(obj, queue, "", key);
    }
    
    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queue) {
        return sendMessage(obj, queue, "", "");
    }
    
    /**
     * 队列消息
     */
    public boolean sendMessage(Object obj, String queue, String tag, String key) {
        Message message = new Message();
        message.setQueue(queue);
        message.setBody(obj);
        if (tag != null) {
            message.setTag(tag);
        }
        if (key != null) {
            message.setKey(key);
        }
        return sendMessage(message);
    }
    
    public boolean sendMessage(Message message) {
        Queue queue = hasQueue(message.getQueue());
        return sendSingleMessage(queue, message, null);
    }
    
    public boolean sendDelayMessage(Message message, Long delayTime) {
        Queue queue = hasDelayQueue(message.getQueue());
        long executorTime = System.currentTimeMillis() + (delayTime);
        return sendSingleMessage(queue, message, executorTime);
    }
    
    public boolean sendTimingMessage(Message message, Long executorTime) {
        Queue queue = hasDelayQueue(message.getQueue());
        return sendSingleMessage(queue, message, executorTime);
    }
    
    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费  一次最多发送100条.超过100条会分批次发送 lua脚本语句长度限制
     */
    public boolean sendBatchMessage(List<?> objs, String queue) {
        return sendBatchMessage(objs, queue, "");
    }
    
    /**
     * 批量一次性打包发送队列消息  消费仍然是一对一消费
     */
    public boolean sendBatchMessage(List<?> objs, String queueName, String tag) {
        if (CollectionUtils.isEmpty(objs)) {
            return true;
        }
        Queue queue = hasQueue(queueName);
        List<Message> messages = new ArrayList<>();
        if (objs.get(0) instanceof Message) {
            messages = (List<Message>) objs;
        } else {
            for (Object obj : objs) {
                Message message = new Message();
                message.setQueue(queueName);
                message.setBody(obj);
                message.setTag(tag);
                messages.add(message);
            }
        }
        return sendBatchMessage(queue, messages);
    }
    
    /**
     * 延迟消息
     */
    public boolean sendDelayMessage(Object obj, String queueName, String tag, String key, Long delayTime) {
        Message message = new Message();
        message.setQueue(queueName);
        message.setBody(obj);
        message.setTag(tag);
        message.setKey(key);
        return sendDelayMessage(message, delayTime);
    }
    
    /**
     * 发送定时消息
     */
    public boolean sendTimingMessage(Object obj, String queueName, String tag, String key, Long executorTime) {
        Message message = new Message();
        message.setQueue(queueName);
        message.setBody(obj);
        message.setTag(tag);
        message.setKey(key);
        return sendTimingMessage(message, executorTime);
    }
    
    /**
     * 单信息
     *
     * @param queue        队列
     * @param message      消息
     * @param executorTime 执行时间
     * @return boolean
     */
    public boolean sendSingleMessage(Queue queue, Message message, Long executorTime) {
        Long increment = increment();
        if (executorTime == null) {
            executorTime = increment;
        }
        
        if (StringUtils.isBlank(message.getVirtualQueueName())) {
            long num;
            if (StringUtils.isNotBlank(message.getKey())) {
                int fnvHash = fnvHash(message.getKey());
                num = fnvHash % queue.getVirtual();
            } else {
                num = increment % queue.getVirtual();
            }
            String virtualQueue = queue.getQueueName() + V_QUEUE_SPLITE + num;
            message.setVirtualQueueName(virtualQueue);
        }
        
        PushMessage pushMessage = new PushMessage();
        pushMessage.setTimestamp(0L);
        pushMessage.setQueue(message.getVirtualQueueName());
        
        SendMessageParam sendMessageParam = new SendMessageParam();
        sendMessageParam.setMessage(message);
        sendMessageParam.setExecutorTime(executorTime);
        return doSendMessage(pushMessage, Collections.singletonList(sendMessageParam));
    }
    
    
    /**
     * 批量发送消息
     *
     * @param queue    队列
     * @param messages 消息
     * @return boolean
     */
    public boolean sendBatchMessage(Queue queue, List<Message> messages) {
        Map<PushMessage, List<SendMessageParam>> messageMap = new HashMap<>();
        for (Message message : messages) {
            //有bug要改成lua
            Long increment = increment();
            Long executorTime = increment;
            if (StringUtils.isBlank(message.getVirtualQueueName())) {
                long num;
                if (StringUtils.isNotBlank(message.getKey())) {
                    int fnvHash = fnvHash(message.getKey());
                    num = fnvHash % queue.getVirtual();
                } else {
                    num = increment % queue.getVirtual();
                }
                String virtualQueue = queue.getQueueName() + V_QUEUE_SPLITE + num;
                message.setVirtualQueueName(virtualQueue);
            }
            PushMessage pushMessage = new PushMessage();
            pushMessage.setTimestamp(0L);
            pushMessage.setQueue(message.getVirtualQueueName());
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
    
    /**
     * 做发送消息
     *
     * @param pushMessage       推送消息
     * @param sendMessageParams 发送消息参数
     * @return boolean
     */
    private boolean doSendMessage(PushMessage pushMessage, List<SendMessageParam> sendMessageParams) {
        //构建redis的请求参数按顺序消息和scope对应
        if (sendMessageParams.size() > 100) {
            //只所以要分离集合发送是因为lua脚本的参数长度有限
            List<List<SendMessageParam>> lists = splitList(sendMessageParams, 100);
            for (List<SendMessageParam> list : lists) {
                return doSendMessage0(pushMessage, list);
            }
        }
        return doSendMessage0(pushMessage, sendMessageParams);
    }
    
    /**
     * 真实发送消息核心方法
     *
     * @param pushMessage       推送消息
     * @param sendMessageParams 发送消息参数
     * @return boolean
     */
    private boolean doSendMessage0(PushMessage pushMessage, List<SendMessageParam> sendMessageParams) {
        List<Message> messageList = sendMessageParams.stream().map(SendMessageParam::getMessage)
                .collect(Collectors.toList());
        //发送前操作
        beforeSend(messageList);
        try {
            
            Boolean success = null;
            Boolean sendAfterCommit = RedisMQDataHelper.get();
            if (sendAfterCommit != null ? sendAfterCommit : GLOBAL_CONFIG.sendAfterCommit) {
                if (TransactionSynchronizationManager.isActualTransactionActive()) {
                    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            boolean success = sendRedisMessage(pushMessage, sendMessageParams);
                            afterSend(messageList, success);
                        }
                    });
                } else if (GLOBAL_CONFIG.seataState && RootContext.inGlobalTransaction()) {
                    seataUtil.registerHook(() -> {
                        boolean success1 = sendRedisMessage(pushMessage, sendMessageParams);
                        afterSend(messageList, success1);
                    });
                } else {
                    success = this.sendRedisMessage(pushMessage, sendMessageParams);
                    afterSend(messageList, success);
                }
            } else {
                success = this.sendRedisMessage(pushMessage, sendMessageParams);
                afterSend(messageList, success);
            }
            return success != null && success;
        } catch (Exception e) {
            onFail(messageList, e);
            log.error("RedisMQProducer doSendMessage Exception:", e);
            return false;
        }
    }
    
    private void afterSend(List<Message> messageList, Boolean success) {
        //发送完成回调
        if (success == null) {
            onFail(messageList, new RedisMqException("success is null"));
        } else {
            if (success) {
                afterSend(messageList);
            } else {
                onFail(messageList, new QueueFullException("RedisMQ Producer Queue Full"));
            }
        }
    }
    
    //底层发送redis消息
    private boolean sendRedisMessage(PushMessage pushMessage, List<SendMessageParam> params) {
        Queue queue = QueueManager.getQueueByVirtual(pushMessage.getQueue());
        Integer queueMaxSize = queue.getQueueMaxSize();
        String lua ="local messageZset = KEYS[1];\n" + "local messageBodyHashKey = KEYS[2];\n"
                + "local queueSize = ARGV[1];\n" + "local size = redis.call('zcard', messageZset);\n"
                + "if size and tonumber(size) >= tonumber(queueSize) then\n" + "return -1;\n" + "end\n"
                + "for i=2, #ARGV, 4 do\n" + "    redis.call('zadd', messageZset, ARGV[i],ARGV[i+1]);\n"
                + "    redis.call(\"hset\", messageBodyHashKey, ARGV[i+2],ARGV[i+3] )\n" + "end\n" + "return size;";
        pushMessage.setQueue(RedisMQConstant.getVQueueNameByVQueue(pushMessage.getQueue()));
        List<String> list = new ArrayList<>();
        //消息详情key名字
        list.add(pushMessage.getQueue());
        list.add(pushMessage.getQueue()+":body");
        Long size = -2L;
        
        //第一个参数是发布订阅的消息
        List<Object> paramsList = new ArrayList<>();
        //队列最大值
        paramsList.add(queueMaxSize);
        for (SendMessageParam param : params) {
            //过期时间
            paramsList.add(param.getExecutorTime());
            //消息
            paramsList.add(param.getMessage().getId());
            paramsList.add(param.getMessage().getId());
            paramsList.add(param.getMessage());
        }
        Object[] objects = paramsList.toArray();
    
        int count = 0;
        while (size == null || count < GLOBAL_CONFIG.producerRetryCount) {
            size = redisMQClientUtil.executeLua(lua, list, objects);
            if (size == null || size < 0) {
                try {
                    Thread.sleep(GLOBAL_CONFIG.producerRetrySleep);
                } catch (InterruptedException ignored) {
                }
                count++;
                log.warn("RedisMQ sendMessage retry");
            } else {
                if (GLOBAL_CONFIG.printProducerLog) {
                    log.info("RedisMQ sendMessage success  message:{}", RedisMQStringMapper.toJsonStr(paramsList));
                }
                //发布订阅
                redisMQClientUtil.publishPullMessage(pushMessage.getQueue());
                return true;
            }
        }
        if (size == -1L) {
            log.error("RedisMQ Producer Queue Full");
            return false;
        }
        return false;
    }
    public ByteBuf encode(Object in)  {
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        try {
            ByteBufOutputStream os = new ByteBufOutputStream(out);
            RedisMQStringMapper.STRING_MAPPER.writeValue((OutputStream) os, in);
            return os.buffer();
        } catch (IOException e) {
            out.release();
        }
        return null;
    }
    private Long increment() {
        String lua = "local count = redis.call('incrBy',KEYS[1],1) " + "if tonumber(count) >= tonumber(ARGV[1]) then "
                + "redis.call('set',KEYS[1],0) " + "count = redis.call('incrBy',KEYS[1],1)" + "end " + "return count;";
        return redisMQClientUtil
                .executeLua(lua, Lists.newArrayList(RedisMQConstant.getSendIncrement()), System.currentTimeMillis());
    }
    
    /**
     * 发送成功后
     *
     * @param message 消息
     */
    private void afterSend(List<Message> message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            try {
                for (ProducerInterceptor interceptor : producerInterceptors) {
                    interceptor.afterSend(message);
                }
            } catch (Exception exception) {
                log.error("RedisMQ afterSend Exception:", exception);
            }
        }
    }
    
    /**
     * 监听失败
     *
     * @param message 消息
     * @param e       e
     */
    private void onFail(List<Message> message, Exception e) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            try {
                for (ProducerInterceptor interceptor : producerInterceptors) {
                    interceptor.onFail(message, e);
                }
            } catch (Exception exception) {
                log.error("RedisMQ onFail interceptor invoke Exception:", exception);
            }
        }
    }
    
    /**
     * 在发送之前
     *
     * @param message 消息
     */
    private void beforeSend(List<Message> message) {
        if (!CollectionUtils.isEmpty(producerInterceptors)) {
            try {
                for (ProducerInterceptor interceptor : producerInterceptors) {
                    interceptor.beforeSend(message);
                }
            } catch (Exception e) {
                log.error("RedisMQ beforeSend Exception:", e);
            }
        }
    }
    
    /*
     * redis的发布订阅  直接传递实际数据即可
     */
    public void publish(String topic, Object obj) {
        redisMQClientUtil.publish(topic, obj);
    }
    
    
    /**
     * 校验队列是否存在
     *
     * @param simpleQueueName
     * @return {@link Queue}
     */
    private Queue hasQueue(String simpleQueueName) {
        Queue queue = QueueManager.getQueue(simpleQueueName);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (queue.isDelayState()) {
            throw new RedisMqException("Redismq Queue type mismatch");
        }
        return queue;
    }
    
    /**
     * 校验延迟队列
     *
     * @param simpleQueueName
     * @return {@link Queue}
     */
    private Queue hasDelayQueue(String simpleQueueName) {
        Queue queue = QueueManager.getQueue(simpleQueueName);
        if (queue == null) {
            throw new RedisMqException("Redismq Can't find queue");
        }
        if (!queue.isDelayState()) {
            throw new RedisMqException("Redismq DelayQueue type mismatch");
        }
        return queue;
    }
    
    /**
     * 试着取消消息
     *
     */
    public Boolean tryCancel(String queueName,String msgId) {
        if (StringUtils.isBlank(queueName)) {
            throw new RedisMqException("tryCancel Virtual Queue is empty");
        }
        Long aLong = redisMQClientUtil.removeMessage(queueName, msgId);
        return aLong>0;
    }
    
    /**
     * 分割列表
     *
     * @param list 列表
     * @param len  len
     * @return {@link List}<{@link List}<{@link T}>>
     */
    private <T> List<List<T>> splitList(List<T> list, int len) {
        if (list == null || list.isEmpty() || len < 1) {
            return Collections.emptyList();
        }
        
        List<List<T>> result = new ArrayList<>();
        
        int size = list.size();
        int count = (size + len - 1) / len;
        
        for (int i = 0; i < count; i++) {
            List<T> subList = list.subList(i * len, (Math.min((i + 1) * len, size)));
            //主要是为了解决，修改新集合会影响老集合的问题
            List<T> newList = new ArrayList<>(subList);
            result.add(newList);
        }
        return result;
    }
    
    public static int fnvHash(String data) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < data.length(); i++) {
            hash = (hash ^ data.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash);
    }
    
    
}
