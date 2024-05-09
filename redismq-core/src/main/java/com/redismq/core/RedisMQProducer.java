package com.redismq.core;

import com.google.common.collect.Lists;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.MessageType;
import com.redismq.common.constant.ProducerAck;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.exception.QueueFullException;
import com.redismq.common.exception.RedisMqException;
import com.redismq.common.pojo.MergedWarpMessage;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.MessageFuture;
import com.redismq.common.pojo.PushMessage;
import com.redismq.common.pojo.Queue;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.id.MsgIDGenerator;
import com.redismq.interceptor.ProducerInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rpc.client.RemotingClient;
import com.redismq.utils.RedisMQDataHelper;
import com.redismq.utils.SeataUtil;
import io.seata.core.context.RootContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.redismq.common.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.GlobalConstant.V_QUEUE_SPLITE;
import static com.redismq.common.constant.MessageType.SEND_MESSAGE_FAIL;
import static com.redismq.common.constant.MessageType.SEND_MESSAGE_SUCCESS;
import static com.redismq.common.constant.RedisMQConstant.NAMESPACE;
import static com.redismq.common.constant.RedisMQConstant.PREFIX;
import static com.redismq.rpc.cache.RpcGlobalCache.FUTURES;

/**
 * @Author: hzh
 * @Date: 2022/5/19 15:46 redismq生产者
 */
public class RedisMQProducer {
    
    protected final Logger log = LoggerFactory.getLogger(RedisMQProducer.class);
    
    private final RedisMQClientUtil redisMQClientUtil;
    
    private final RemotingClient remotingClient;
    
    private List<ProducerInterceptor> producerInterceptors;
    
    private final BlockingQueue<Message> basket = new LinkedBlockingQueue<>();
    
    private SeataUtil seataUtil;
    
    private final Object mergeLock = new Object();
    
    private final int MAX_MERGE_SEND_MILLS = GlobalConfigCache.NETTY_CONFIG.getMaxMergeSendMills();
    
    private boolean isSending = false;
    
    private final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1);
    
    private ExecutorService mergeSendExecutorService;
    
    public List<ProducerInterceptor> getProducerInterceptors() {
        return producerInterceptors;
    }
    
    public void setProducerInterceptors(List<ProducerInterceptor> producerInterceptors) {
        this.producerInterceptors = producerInterceptors;
    }
    
    
    public RedisMQProducer(RedisMQClientUtil redisMQClientUtil, RemotingClient remotingClient) {
        this.redisMQClientUtil = redisMQClientUtil;
        if (GLOBAL_CONFIG.seataState) {
            this.seataUtil = new SeataUtil();
        }
        this.remotingClient = remotingClient;
    }
    
    /**
     * 初始化
     */
    @PostConstruct
    public void init() {
        mergeSendExecutorService = new ThreadPoolExecutor(1, 1, 60, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000));
        mergeSendExecutorService.submit(new MergedSendRunnable());
        
        // 清理超时任务
        timerExecutor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, MessageFuture> entry : FUTURES.entrySet()) {
                MessageFuture future = entry.getValue();
                if (future.isTimeout()) {
                    FUTURES.remove(entry.getKey());
                    Message rpcMessage = future.getMessage();
                    future.setResultMessage(new TimeoutException(
                            String.format("msgId: %s ,msgType: %s ,msg: %s ,request timeout", rpcMessage.getId(),
                                    rpcMessage.getBody().toString())));
                }
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }
    
    @PreDestroy
    public void destroy() {
        timerExecutor.shutdown();
        mergeSendExecutorService.shutdown();
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
    //    public boolean sendBatchMessage(List<?> objs, String queue) {
    //        return sendBatchMessage(objs, queue, "");
    //    }
    //
    //    /**
    //     * 批量一次性打包发送队列消息  消费仍然是一对一消费
    //     */
    //    public boolean sendBatchMessage(List<?> objs, String queueName, String tag) {
    //        if (CollectionUtils.isEmpty(objs)) {
    //            return true;
    //        }
    //        Queue queue = hasQueue(queueName);
    //        List<Message> messages = new ArrayList<>();
    //        if (objs.get(0) instanceof Message) {
    //            messages = (List<Message>) objs;
    //        } else {
    //            for (Object obj : objs) {
    //                Message message = new Message();
    //                message.setQueue(queueName);
    //                message.setBody(obj);
    //                message.setTag(tag);
    //                messages.add(message);
    //            }
    //        }
    //        return sendBatchMessage(queue, messages);
    //    }
    
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
        Long increment = increment(queue);
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
        message.setOffset(increment);
        message.setExecuteTime(executorTime);
        
        return sendOffMessage(message);
    }
    
    /**
     * 批量发送消息
     *
     * @param queue    队列
     * @param messages 消息
     * @return boolean
     */
    //    public boolean sendBatchMessage(Queue queue, List<Message> messages) {
    //        Map<PushMessage, List<SendMessageParam>> messageMap = new HashMap<>();
    //        for (Message message : messages) {
    //            //有bug要改成lua
    //            Long increment = increment(queue);
    //            Long executorTime = increment;
    //            if (StringUtils.isBlank(message.getVirtualQueueName())) {
    //                long num;
    //                if (StringUtils.isNotBlank(message.getKey())) {
    //                    int fnvHash = fnvHash(message.getKey());
    //                    num = fnvHash % queue.getVirtual();
    //                } else {
    //                    num = increment % queue.getVirtual();
    //                }
    //                String virtualQueue = queue.getQueueName() + V_QUEUE_SPLITE + num;
    //                message.setVirtualQueueName(virtualQueue);
    //            }
    //            message.setOffset(increment);
    //            message.setExecuteTime(executorTime);
    //            PushMessage pushMessage = new PushMessage();
    //            pushMessage.setTimestamp(0L);
    //            pushMessage.setQueue(message.getVirtualQueueName());
    //            SendMessageParam sendMessageParam = new SendMessageParam();
    //            sendMessageParam.setMessage(message);
    //            sendMessageParam.setExecutorTime(executorTime);
    //            List<SendMessageParam> messageList = messageMap.computeIfAbsent(pushMessage, m -> new ArrayList<>());
    //            messageList.add(sendMessageParam);
    //        }
    //        // 批次发送有一次失败即为失败
    //        AtomicBoolean result = new AtomicBoolean(true);
    //        messageMap.forEach((pushMessage, sendMessageParams) -> {
    //            boolean success = this.sendMessage(pushMessage, sendMessageParams);
    //            if (!success) {
    //                result.set(false);
    //            }
    //        });
    //        return result.get();
    //    }
    
    /**
     * 做发送消息
     *
     * @return boolean
     */
    private boolean sendOffMessage(Message message) {
        message.setId(MsgIDGenerator.generateIdStr());
        
        long timeoutMillis = GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout();
        
        MessageFuture messageFuture = new MessageFuture();
        messageFuture.setMessage(message);
        messageFuture.setTimeout(timeoutMillis);
        FUTURES.put(message.getId(), messageFuture);
        
        //需要处理成如果队列满了的话阻塞等待
        if (!basket.offer(message)) {
            log.error("MQProducer put message into basketMap offer failed, queue :{}, message:{}",
                    message.getVirtualQueueName(), message);
            return false;
        }
        if (!isSending) {
            synchronized (mergeLock) {
                mergeLock.notifyAll();
            }
        }
        
        //同步阻塞等待响应结果
        
        try {
            Object result = messageFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
            if (result != null && result.equals(true)) {
                return true;
            } else if (result instanceof Exception) {
                throw (Exception) result;
            } else {
                throw new RedisMqException("doSendMessage  result :" + result);
            }
        } catch (Exception exx) {
            log.error("doSendMessage wait response error message:  :{} error:{} ", message, exx.getMessage());
            throw new RedisMqException("doSendMessage ", exx);
        }
    }
    
    /**
     * 真实发送消息核心方法
     */
    private void doSend(MergedWarpMessage mergedWarpMessage) {
        Boolean sendAfterCommit = RedisMQDataHelper.get();
        //发送前操作
        List<Message> messages = mergedWarpMessage.getMessages();
        beforeSend(messages);
        
        // 如果是异步确认
        if (GLOBAL_CONFIG.getProductAck().equals(ProducerAck.ASYNC)) {
            for (Message message : messages) {
                if (remotingClient != null) {
                    remotingClient.sendAsync(message, MessageType.CREATE_MESSAGE);
                }
            }
        } else {
            //同步确认
            if (remotingClient != null) {
                remotingClient.sendBatchSync(messages, MessageType.CREATE_MESSAGE);
            }
        }
        
        if (sendAfterCommit != null ? sendAfterCommit : GLOBAL_CONFIG.sendAfterCommit) {
            if (TransactionSynchronizationManager.isActualTransactionActive()) {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        doSendMessage(mergedWarpMessage);
                        
                    }
                });
            } else if (GLOBAL_CONFIG.seataState && RootContext.inGlobalTransaction()) {
                seataUtil.registerHook(() -> {
                    doSendMessage(mergedWarpMessage);
                    
                });
            } else {
                this.doSendMessage(mergedWarpMessage);
            }
        } else {
            this.doSendMessage(mergedWarpMessage);
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
    private boolean doSendMessage(MergedWarpMessage mergedWarpMessage) {
        List<Message> messages = mergedWarpMessage.getMessages();
        
        PushMessage pushMessage = mergedWarpMessage.getPushMessage();
        Queue queue = QueueManager.getQueueByVirtual(pushMessage.getQueue());
        Integer queueMaxSize = queue.getQueueMaxSize();
        String lua = "local messageZset = KEYS[1];\n" + "local messageBodyHashKey = KEYS[2];\n"
                + "local queueSize = ARGV[1];\n" + "local size = redis.call('zcard', messageZset);\n"
                + "if size and tonumber(size) >= tonumber(queueSize) then\n" + "return -1;\n" + "end\n"
                + "for i=2, #ARGV, 4 do\n" + "    redis.call('zadd', messageZset, ARGV[i],ARGV[i+1]);\n"
                + "    redis.call(\"hset\", messageBodyHashKey, ARGV[i+2],ARGV[i+3] )\n" + "end\n" + "return size;";
        pushMessage.setQueue(RedisMQConstant.getVQueueNameByVQueue(pushMessage.getQueue()));
        List<String> list = new ArrayList<>();
        //消息详情key名字
        list.add(pushMessage.getQueue());
        list.add(pushMessage.getQueue() + ":body");
        Long size = -2L;
        
        //第一个参数是发布订阅的消息
        List<Object> paramsList = new ArrayList<>();
        //队列最大值
        paramsList.add(queueMaxSize);
        for (Message param : messages) {
            //过期时间
            paramsList.add(param.getExecuteTime());
            //消息
            paramsList.add(param.getId());
            paramsList.add(param.getId());
            paramsList.add(param);
        }
        Object[] objects = paramsList.toArray();
        
        int count = 0;
        boolean success = false;
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
                redisMQClientUtil.publishPullMessage(pushMessage);
                success = true;
                break;
            }
        }
        
        if (!success) {
            log.error("RedisMQ Producer Queue Full");
        }
        
        // 如果是异步确认
        if (GLOBAL_CONFIG.getProductAck().equals(ProducerAck.ASYNC)) {
            for (Message message : messages) {
                if (remotingClient != null) {
                    String messageId = message.getId();
                    remotingClient.sendAsync(messageId, success ? SEND_MESSAGE_SUCCESS : SEND_MESSAGE_FAIL);
                }
            }
            setResults(messages, success ? true : new QueueFullException("RedisMQ Producer Queue Full"));
        } else {
            //同步确认
            if (remotingClient != null) {
                List<String> msgIds = messages.stream().map(Message::getId).collect(Collectors.toList());
                remotingClient.sendBatchSync(msgIds, success ? SEND_MESSAGE_SUCCESS : SEND_MESSAGE_FAIL);
            }
            setResults(messages, success ? true : new QueueFullException("RedisMQ Producer Queue Full"));
        }
        
        // 发送后钩子函数
        afterSend(messages, success);
        
        return success;
    }
    
    /**
     * 设置发送结果
     *
     * @param messages 信息
     */
    private void setResults(List<Message> messages, Object result) {
        for (Message message : messages) {
            MessageFuture messageFuture = FUTURES.remove(message.getId());
            if (messageFuture != null) {
                messageFuture.setResultMessage(result);
            }
        }
    }
    
    private Long increment(Queue queue) {
        String queueOffset = PREFIX + NAMESPACE + SPLITE + "offset" + SPLITE + queue.getQueueName();
        String lua = "local count = redis.call('incrBy',KEYS[1],1) " + "if tonumber(count) >= tonumber(ARGV[1]) then "
                + "redis.call('set',KEYS[1],0) " + "count = redis.call('incrBy',KEYS[1],1)" + "end " + "return count;";
        Long num = redisMQClientUtil.executeLua(lua, Lists.newArrayList(queueOffset), System.currentTimeMillis());
        return num;
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
     */
    public Boolean tryCancel(String queueName, String msgId) {
        if (StringUtils.isBlank(queueName)) {
            throw new RedisMqException("tryCancel Virtual Queue is empty");
        }
        Boolean aBoolean = redisMQClientUtil.removeMessage(queueName, msgId);
        return aBoolean;
    }
    
    
    private class MergedSendRunnable implements Runnable {
        
        @Override
        public void run() {
            while (true) {
                synchronized (mergeLock) {
                    try {
                        mergeLock.wait(MAX_MERGE_SEND_MILLS);
                    } catch (InterruptedException e) {
                    }
                }
                
                isSending = true;
                
                Map<String, MergedWarpMessage> map = new HashMap<>();
                
                while (!basket.isEmpty()) {
                    Message msg = basket.poll();
                    if (msg == null) {
                        continue;
                    }
                    MergedWarpMessage mergedWarpMessage = map
                            .computeIfAbsent(msg.getVirtualQueueName(), a -> new MergedWarpMessage());
                    List<Message> messages = mergedWarpMessage.getMessages();
                    messages.add(msg);
                    //超过指定数量直接发送
                    if (messages.size() >= GLOBAL_CONFIG.producerMaxBatchSize) {
                        break;
                    }
                }
                
                map.forEach((queue, mergeMsg) -> {
                    try {
                        PushMessage pushMessage = new PushMessage();
                        pushMessage.setQueue(queue);
                        pushMessage.setTimestamp(0L);
                        //订阅推送设置最早执行的时间
                        mergeMsg.getMessages().stream().map(Message::getExecuteTime).min(Long::compareTo)
                                .ifPresent(pushMessage::setTimestamp);
                        mergeMsg.setPushMessage(pushMessage);
                        //拆分队列发送
                        doSend(mergeMsg);
                    } catch (Exception e) {
                        log.error("mergeSend call failed: {}", e.getMessage(), e);
                        for (Message message : mergeMsg.getMessages()) {
                            String id = message.getId();
                            MessageFuture messageFuture = FUTURES.remove(id);
                            if (messageFuture != null) {
                                messageFuture.setResultMessage(
                                        new RedisMqException(String.format("%s mergedSend error", id), e));
                            }
                        }
                    }
                    ;
                });
                isSending = false;
            }
        }
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
