package com.redismq.container;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.AckMode;
import com.redismq.common.constant.MessageType;
import com.redismq.common.exception.RedisMqException;
import com.redismq.common.pojo.GroupOffsetQeueryMessageDTO;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.Queue;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.core.RedisListenerCallable;
import com.redismq.delay.DelayTimeoutTask;
import com.redismq.interceptor.ConsumeInterceptor;
import com.redismq.queue.QueueManager;
import com.redismq.rpc.client.RemotingClient;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.redismq.common.config.GlobalConfigCache.GLOBAL_CONFIG;
import static com.redismq.common.constant.GlobalConstant.THREAD_NUM_MAX;
import static com.redismq.common.constant.GlobalConstant.WORK_THREAD_STOP_WAIT;
import static com.redismq.common.constant.RedisMQConstant.getVirtualQueueLock;
import static com.redismq.queue.QueueManager.INVOKE_VIRTUAL_QUEUES;


/**
 * @author hzh
 * @date 2021/8/10 redis延时队列实现   通过发布订阅和时间轮实现高性能。  一个queue对应一个端点对应多个queue:tag
 */
public class RedisMQListenerContainer extends AbstractMessageListenerContainer {
    
    protected static final Logger log = LoggerFactory.getLogger(RedisMQListenerContainer.class);
    
    /**
     * 延长锁看门狗
     */
    private final ScheduledThreadPoolExecutor lifeExtensionThread = new ScheduledThreadPoolExecutor(1);
    
    private volatile ScheduledFuture<?> scheduledFuture;
    
    private final ThreadPoolExecutor work;
    private RemotingClient remotingClient;
    
    /**
     * 分组内最后的偏移量
     */
    protected long lastGroupOffset = 0;
    /**
     * 启动时队列的最新偏移量
     */
    protected long lastOffset = 0;
    /**
     * 是否拉取偏移量差值的消息
     * 消费者偏移量是否超过允许的最大值。如果超过-拉取消息一直到当前偏移量
     */
    private  boolean pullOffsetLow;
    /**
     * 停止
     */
    @Override
    public void doStop() {
        work.shutdown();
        delayTimeoutTaskManager.stop();
        try {
            if (!work.awaitTermination(WORK_THREAD_STOP_WAIT, TimeUnit.SECONDS)) {
                log.warn("redismq workThreadPool shutdown timeout");
                work.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("redismq workThreadPool shutdown error", e);
            Thread.currentThread().interrupt();
        }
    }
    
    public RedisMQListenerContainer(RedisMQClientUtil redisMQClientUtil, Queue queue,
            List<ConsumeInterceptor> consumeInterceptorList,RemotingClient remotingClient,long lastGroupOffset,long lastOffset) {
        super(redisMQClientUtil, queue, consumeInterceptorList );
        lifeExtension();
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(), 60L, TimeUnit.SECONDS,
                // 这个范围内的视为核心线程可以处理 队列的数量
                new LinkedBlockingQueue<>(getConcurrency() << 3), new ThreadFactory() {
            private final ThreadGroup group;
            
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            private final String NAME_PREFIX = "REDISMQ-WORK-" + queue.getQueueName() + "-";
            
            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }
            
            @Override
            public Thread newThread(Runnable r) {
                //除了固定的boss线程。临时新增的线程会删除了会递增，int递增有最大值。这里再9999的时候就从固定线程的数量上重新计算.防止线程名字过长
                int current = threadNumber
                        .getAndUpdate(operand -> operand >= THREAD_NUM_MAX ? getConcurrency() + 1 : operand + 1);
                Thread t = new Thread(group, r, NAME_PREFIX + current);
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
        this.lastGroupOffset=lastGroupOffset;
        this.lastOffset=lastOffset;
        pullOffsetLow =  lastOffset - lastGroupOffset > GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax();
        this.remotingClient=remotingClient;
    }
    
    
    /**
     * 拉取队列消息
     *
     * @param vQueueName 队列名称
     * @return {@link Set}<{@link Long}>
     */
    public Set<Long> pull(String vQueueName) {
        Set<Long> delayTimes = new LinkedHashSet<>();
        List<Future<Message>> futures = new ArrayList<>();
        while (isRunning()) {
            try {
                //获取已经到时间要执行的任务  本地消息的数量相当于本地偏移量   localMessages.size()是指从这个位置之后开始啦
                long pullTime = System.currentTimeMillis();
                
                int pullSize = super.maxConcurrency - futures.size();
                
                //说明消费队列已经满了 等待所有任务消费完成，然后再继续拉取消息.后面优化加个最长等待时间。是针对每个任务的。可以动态控制。如果超时的话任务就取消丢弃。
                if (pullSize <= 0) {
                    pullSize = waitConsume(vQueueName,futures, GLOBAL_CONFIG.getTaskTimeout(), true);
                }
                //延时队列 必须等待执行完成后才能获取下一次的消息
                
                if (!futures.isEmpty()) {
                    pullSize = waitConsume(vQueueName, futures, GLOBAL_CONFIG.getTaskTimeout(), true);
                }
           
                
                
                // 先获取偏移量落后的group的持久化的message
                List<Message> messages = getOffsetLowStoreMessage(vQueueName);
                
                // 从redis中获取消息
                if (CollectionUtils.isEmpty(messages)){
                    long startScore=0;
//                    if (!delay){
//                        startScore=lastGroupOffset;
//                    }
                    messages = redisMQClientUtil.pullMessage(vQueueName, startScore,pullTime, 0, pullSize);
                }
                
                if (CollectionUtils.isEmpty(messages)) {
                    //响应中断
                    if (!isRunning()) {
                        return delayTimes;
                    }
                    //消息已经拉不到了。如果消费未完成 等待1秒钟消费完成，如果1秒没有消费完。再继续拉取消息，因为有可能有其他新的消息进来。
                    if (!futures.isEmpty()) {
                        waitConsume(vQueueName,futures, GLOBAL_CONFIG.getTaskWaitTime(), false);
                        continue;
                    }
                    
                    if (delay) {
                        //如果没有数据获取头部数据100条的时间.加入时间轮.到点的时候再过来取真实数据
                        List<Pair<Message, Double>> pairs = redisMQClientUtil
                                .pullMessageByTimeWithScope(vQueueName, pullTime, 0, GLOBAL_CONFIG.delayQueuePullSize);
                        pairs.forEach((pair -> delayTimes.add(pair.getValue().longValue())));
                    }
                    break;
                }
              
                List<RedisListenerCallable> callableInvokes = new ArrayList<>();
                for (Message message : messages) {
                    if (!isRunning()) {
                        break;
                    }
                    if (message == null) {
                        continue;
                    }
                    
                    try {
                        //手动ack
                        if (AckMode.MAUAL.equals(ackMode)) {
                        
                        } else {
//                            Boolean remove = redisMQClientUtil.ackMessage(vQueueName, message.getId(),message.getOffset());
//                            if (!remove) {
//                                continue;
//                            }
                        }
                        String id = super.getRunableKey(message.getTag());
                        RedisListenerCallable callable = super.getRedisListenerCallable(id, message);
                        if (callable == null) {
                            // 如果是框架中的异常,说明异常是不可修复的.删除异常的消息
                            redisMQClientUtil.removeMessage(vQueueName, message.getId());
                            log.error("RedisMqException   not found queue or tag removeMessage:{}",
                                    RedisMQStringMapper.toJsonStr(message));
                            continue;
                        }
                        
                        callableInvokes.add(callable);
                        //不是延时队列的话记录偏移量
                        if (!delay){
                            Message lastMsg = messages.get(messages.size() - 1);
                            // 获取最后一个消息的偏移量的下一个值
                            lastGroupOffset = Math.max(lastGroupOffset,lastMsg.getOffset() +1) ;
                        }
                    } catch (Throwable e) {
                        if (isRunning()) {
                            log.error("redisMQ listener container error ", e);
                        }
                    }
                }
                if (CollectionUtils.isEmpty(callableInvokes)) {
                    if (isRunning()) {
                        log.error("redisMQ callableInvokes isEmpty queueName:{}", vQueueName);
                    }
                    continue;
                }
                
                for (RedisListenerCallable callableInvoke : callableInvokes) {
                    Future<Message> submit = work.submit(callableInvoke);
                    futures.add(submit);
                }
            } catch (Throwable e) {
                if (isRunning()) {
                    //报错需要  semaphore.release();
                    log.error("redisMQ pop error", e);
                    if (e.getMessage().contains("WRONGTYPE Operation against a key holding the wrong kind of value")) {
                        log.error("redisMQ [ERROR] queue not is zset type。 cancel pop");
                        stop();
                    }
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        return delayTimes;
    }
    
    /**
     * 获取偏移量落后的队列消息
     * @param vQueueName
     * @return
     */
    private  List<Message> getOffsetLowStoreMessage(String vQueueName) {
        if (!pullOffsetLow){
            return null;
        }
        
        if (remotingClient == null){
            log.warn("remotingClient not register not getOffsetLowStoreMessage please open spring.redismq.netty-config.client.enable=true");
            return null;
        }
        
        List<Message> messages = new ArrayList<>();
        long diff = lastOffset - lastGroupOffset;
        if (diff <=0){
            //如果消息偏移量已经消费完了
            //获取最新偏移量看下差距
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(queueName);
            //如果最新偏移量的差距又超过了偏差值
            if (queueMaxOffset-lastGroupOffset > GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax()){
                log.info("vQueueName:{} groupId:{} lastOffsetRefresh oldLastOffset:{} queueMaxOffset:{}",
                        vQueueName,GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),lastOffset,queueMaxOffset);
                lastOffset = queueMaxOffset;
                diff = lastOffset - lastGroupOffset;
            }else{
                pullOffsetLow = false;
                return null;
            }
        }
        if (GlobalConfigCache.NETTY_CONFIG.getServer().isEnable() && diff > 0){
            log.info("vQueueName:{} groupId:{} diff:{} doPullMessage",vQueueName,GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),diff);
            GroupOffsetQeueryMessageDTO offsetDTO = new GroupOffsetQeueryMessageDTO();
            offsetDTO.setOffset(lastGroupOffset);
            offsetDTO.setVQueue(vQueueName);
            offsetDTO.setLastOffset(lastOffset);
            //根据偏移量获取 100条滞后消息 如果全获取 内存不够
            String object = (String) remotingClient.sendSync(offsetDTO, MessageType.GET_QUEUE_MESSAGE_BY_OFFSET);
            if (object == null){
                return null;
            }
            List<Map> list = RedisMQStringMapper.toList(object, Map.class);
            if (!CollectionUtils.isEmpty(list)) {
                log.info("Consumer Group Offset Low RetryConsumer  \n lastGroupOffset :{} lastOffset:{}  \n Message:{}",
                        lastGroupOffset,lastOffset, list);
                for (Map map : list) {
                    String jsonStr = RedisMQStringMapper.toJsonStr(map);
                    Message msg = RedisMQStringMapper.toBean(jsonStr, Message.class);
                    messages.add(msg);
                }
            }else{
                // 根据偏移量拉不到消息，注意这部分消息将丢失。 数据库里也被删除了
                log.error("Consumer Set lastOffset to lastGroupOffset Because persistence"
                        + " message isEmpty lastGroupOffset :{} lastOffset:{}",lastGroupOffset,lastOffset);
                log.error("Message Loss !!!! vQueueName  lastGroupOffset:{} lastOffset:{}",lastGroupOffset,lastOffset);
            }
        }
        if (!CollectionUtils.isEmpty(messages)) {
            log.info("Group offset Low groupId :{} lastGroupOffset :{} messageIds:{}",
                    GlobalConfigCache.CONSUMER_CONFIG.getGroupId(), lastGroupOffset,
                    messages.stream().map(Message::getId).collect(Collectors.toList()));
        }
       
        return messages;
    }
    
    /**
     * 等待消费任务完成
     *
     * @return int
     */
    private int waitConsume(String vQueueName,List<Future<Message>> futures, long milliseconds, boolean timeoutDrop) {
        List<Message> messageList = new ArrayList<>();
        for (Future<Message> future : futures) {
            try {
                Message msg = future.get(milliseconds, TimeUnit.MILLISECONDS);
                messageList.add(msg);
            } catch (Exception e) {
                log.error("redisMQ waitConsume error", e);
            }
        }
        
        if (timeoutDrop) {
            futures.clear();
        }
        
        int pullSize;
        pullSize = super.maxConcurrency;
        if (CollectionUtils.isEmpty(messageList)) {
            return pullSize;
        }
        if (!timeoutDrop) {
            futures.removeIf(Future::isDone);
        }
        
        ackMessage(vQueueName, messageList);
        
        messageList.clear();
        return pullSize;
    }
    
    private void ackMessage(String vQueueName, List<Message> messageList) {
        if (!messageList.isEmpty()){
            Long offset = messageList.stream().map(Message::getOffset).max(Long::compareTo).get();
            String msgIds = messageList.stream().map(Message::getId).collect(Collectors.joining(","));
             redisMQClientUtil.ackBatchMessage(vQueueName,msgIds,offset);
        }
    }
    
    
    @Override
    public void repush() {
        throw new RedisMqException("延时队列不存在的方法  repush()");
    }
    
    
    /**
     * 开始拉取消息的任务。同一个时间一个虚拟队列只允许一个服务的一个线程进行拉取操作
     *
     * @param virtualQueue 虚拟队列
     * @param startTime    开始时间
     */
    public void start(String virtualQueue, Long startTime) {
        
        running();

        //为空说明当前能获取到数据
        DelayTimeoutTask timeoutTask = delayTimeoutTaskManager
                .computeIfAbsent(virtualQueue, task -> new DelayTimeoutTask(virtualQueue,redisMQClientUtil) {
                    @Override
                    protected Set<Long> pullTask() {
                        try {
                            List<String> virtualQueues = QueueManager.getCurrentVirtualQueues().get(queueName);
                            if (CollectionUtils.isEmpty(virtualQueues)) {
                                return null;
                            }
                            if (!virtualQueues.contains(virtualQueue)) {
                                return null;
                            }
                            //添加到当前执行队列。看门狗用
                            INVOKE_VIRTUAL_QUEUES.add(virtualQueue);
                            Set<Long> delayTimes = pull(virtualQueue);
                            //为空说明当前能获取到数据
                            return new LinkedHashSet<>(delayTimes);
                        } finally {
                            INVOKE_VIRTUAL_QUEUES.remove(virtualQueue);
                        }
                    }
                });
        try {
            delayTimeoutTaskManager.schedule(timeoutTask, startTime);
        } catch (Exception e) {
            log.error("delayTimeoutTaskManager schedule ", e);
        }
    }
    


    
    /**
     * 消费锁续期 看门狗
     */
    private void lifeExtension() {
        if (scheduledFuture == null || scheduledFuture.isCancelled()) {
            scheduledFuture = lifeExtensionThread.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    List<String> virtualQueues = QueueManager.getCurrentVirtualQueues().get(queueName);
                    if (CollectionUtils.isEmpty(virtualQueues)) {
                        return;
                    }
                    for (String virtualQueue : INVOKE_VIRTUAL_QUEUES) {
                        String lua = "if (redis.call('exists', KEYS[1]) == 1) then " + "redis.call('expire', KEYS[1],"
                                + GLOBAL_CONFIG.virtualLockTime + "); " + "return 1; " + "end; " + "return 0;";
                        try {
                            List<String> list = new ArrayList<>();
                            list.add(getVirtualQueueLock(virtualQueue));
                            Long success = redisMQClientUtil.executeLua(lua, list);
                        } catch (Exception e) {
                            if (isRunning()) {
                                log.error("lifeExtension  redisTemplate.expire Exception", e);
                            }
                        }
                    }
                }
            }, GLOBAL_CONFIG.virtualLockWatchDogTime, GLOBAL_CONFIG.virtualLockWatchDogTime, TimeUnit.SECONDS);
        }
    }
}

