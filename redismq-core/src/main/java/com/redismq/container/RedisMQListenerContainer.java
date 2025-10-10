package com.redismq.container;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.AckMode;
import com.redismq.common.constant.MessageType;
import com.redismq.common.constant.OffsetEnum;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
 * Redis消息队列监听容器 - RedisMQ核心消费组件
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>消息拉取：从Redis拉取消息并提交到线程池执行</li>
 *   <li>偏移量管理：维护消费者组的消息偏移量，支持顺序消费和At-least-once语义</li>
 *   <li>延时队列：支持延时消息的时间轮调度</li>
 *   <li>重复消费防护：通过pullingMessageIds集合防止消息重复拉取</li>
 *   <li>消息超时处理：极端超时（10分钟）的消息自动移至死信队列</li>
 *   <li>负载保护：偏移量窗口大小限制，防止内存溢出</li>
 * </ul>
 *
 * <h2>消息消费流程</h2>
 * <pre>
 * 1. 拉取消息：从Redis ZSET拉取到达时间的消息
 * 2. 去重过滤：过滤掉正在处理中的消息（pullingMessageIds）
 * 3. 记录状态：将消息记录到offsetWindow，标记为未完成
 * 4. 提交执行：提交到work线程池异步执行
 * 5. 完成标记：消费完成后标记offsetWindow中对应消息为finished
 * 6. 偏移量提交：查找连续完成的偏移量，批量ACK到Redis
 * 7. 清理状态：从offsetWindow和pullingMessageIds中移除已完成消息
 * </pre>
 *
 * <h2>偏移量管理机制</h2>
 * <p>采用"连续偏移量提交"策略，确保消息顺序消费：</p>
 * <ul>
 *   <li>committedOffset：已提交的最大连续偏移量</li>
 *   <li>offsetWindow：SortedMap，维护消息完成状态</li>
 *   <li>只提交连续完成的偏移量，避免偏移量空洞</li>
 *   <li>示例：offset 1,2,3,5完成，4未完成 → 只提交1,2,3</li>
 * </ul>
 *
 * <h2>极端超时处理</h2>
 * <p>防止慢消息阻塞队列消费：</p>
 * <ul>
 *   <li>正常超时（120秒）：任务继续在后台执行</li>
 *   <li>极端超时（600秒）：标记为finished，移至死信队列，允许偏移量前进</li>
 *   <li>死信队列命名：PREFIX:NAMESPACE:DEAD:{queueName}</li>
 * </ul>
 *
 * <h2>内存保护机制</h2>
 * <ul>
 *   <li>offsetWindow最大1000条，达到上限时等待消费完成</li>
 *   <li>pullingMessageIds自然受maxConcurrency限制</li>
 *   <li>达到80%（800条）时warn告警</li>
 * </ul>
 *
 * <h2>并发控制</h2>
 * <ul>
 *   <li>work线程池：核心线程数=concurrency，最大=maxConcurrency</li>
 *   <li>offsetWindow：SortedMap + synchronized保证线程安全</li>
 *   <li>pullingMessageIds：ConcurrentHashMap.newKeySet()保证并发安全</li>
 * </ul>
 *
 * <h2>延时队列特殊处理</h2>
 * <p>延时队列（delay=true）的行为与普通队列不同：</p>
 * <ul>
 *   <li>必须等待所有任务完成才能拉取下一批</li>
 *   <li>不使用offsetWindow和pullingMessageIds</li>
 *   <li>使用时间轮+发布订阅实现高性能延时</li>
 * </ul>
 *
 * <h2>配置参数</h2>
 * <ul>
 *   <li>concurrency：核心线程数，队列的并发消费数</li>
 *   <li>maxConcurrency：最大线程数</li>
 *   <li>GLOBAL_CONFIG.taskTimeout：任务正常超时时间（默认120秒）</li>
 *   <li>GLOBAL_CONFIG.taskWaitTime：空闲等待时间（默认1秒）</li>
 *   <li>MAX_OFFSET_WINDOW_SIZE：偏移量窗口大小（1000）</li>
 *   <li>EXTREME_TIMEOUT：极端超时时间（600秒）</li>
 * </ul>
 *
 * <h2>消息语义保证</h2>
 * <p>At-least-once：消息至少被消费一次，业务需实现幂等</p>
 * <ul>
 *   <li>正常场景：消息消费一次</li>
 *   <li>重启/重平衡：offsetWindow中未提交的消息会被重新消费</li>
 *   <li>极端超时后重启：已移至死信队列的消息不会重复消费</li>
 * </ul>
 *
 * @author hzh
 * @date 2021/8/10
 * @see AbstractMessageListenerContainer
 * @see MessageStatus
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
     * 虚拟队列偏移量管理器（每个虚拟队列独立管理offset、window、消息ID）
     */
    private static class VirtualQueueOffsetManager {
        /** 该虚拟队列已提交的最大连续偏移量 */
        volatile long committedOffset = 0;
        /** 偏移量窗口（维护消息完成状态） */
        final SortedMap<Long, MessageStatus> offsetWindow = Collections.synchronizedSortedMap(new TreeMap<>());
        /** 正在拉取的消息ID集合（防止重复拉取） */
        final Set<String> pullingMessageIds = ConcurrentHashMap.newKeySet();
        /** 是否需要从持久化存储追赶offset */
        volatile boolean pullOffsetLow = false;
    }

    /**
     * 虚拟队列偏移量管理器映射表（key=虚拟队列名）
     */
    private final Map<String, VirtualQueueOffsetManager> vqOffsetManagers = new ConcurrentHashMap<>();
    
    /**
     * 窗口最大大小限制（防止内存溢出）
     */
    private static final int MAX_OFFSET_WINDOW_SIZE = 1000;
    
    /**
     * 窗口大小告警阈值（达到80%时告警）
     */
    private static final int OFFSET_WINDOW_WARN_SIZE = (int) (MAX_OFFSET_WINDOW_SIZE * 0.8);
    
    /**
     * 极端超时时间（10分钟，600000毫秒）
     * 超过此时间的消息将被移到死信队列
     */
    private static final long EXTREME_TIMEOUT = 600000;
    
    /**
     * 消息状态类
     */
    private static class MessageStatus {
        final String messageId;
        final Message message;
        volatile boolean finished;
        final long pullTime;
        
        MessageStatus(String messageId, Message message) {
            this.messageId = messageId;
            this.message = message;
            this.finished = false;
            this.pullTime = System.currentTimeMillis();
        }
        
        boolean isExtremeTimeout(long timeout) {
            return !finished && (System.currentTimeMillis() - pullTime > timeout);
        }
    }
    
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
            List<ConsumeInterceptor> consumeInterceptorList, RemotingClient remotingClient) {
        super(redisMQClientUtil, queue, consumeInterceptorList);
        lifeExtension();
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(), 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(getConcurrency() << 3),
                createThreadFactory(queue.getQueueName()),
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.remotingClient = remotingClient;
        // 注意：不在构造函数中初始化offset，改为在getOrCreateVQManager中动态获取
    }
    
    /**
     * 创建线程工厂
     *
     * @param queueName 队列名称
     * @return ThreadFactory实例
     */
    private ThreadFactory createThreadFactory(String queueName) {
        return new ThreadFactory() {
            private final ThreadGroup group;
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final String NAME_PREFIX = "REDISMQ-WORK-" + queueName + "-";
            
            {
                SecurityManager s = System.getSecurityManager();
                group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            }
            
            @Override
            public Thread newThread(Runnable r) {
                // 线程编号递增，达到THREAD_NUM_MAX时重置为concurrency+1，防止线程名过长
                int current = threadNumber
                        .getAndUpdate(operand -> operand >= THREAD_NUM_MAX ? getConcurrency() + 1 : operand + 1);
                Thread t = new Thread(group, r, NAME_PREFIX + current);
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        };
    }

    /**
     * 获取或创建虚拟队列的偏移量管理器
     *
     * @param vQueueName 虚拟队列名
     * @return 虚拟队列偏移量管理器
     */
    private VirtualQueueOffsetManager getOrCreateVQManager(String vQueueName) {
        return vqOffsetManagers.computeIfAbsent(vQueueName, vq -> {
            VirtualQueueOffsetManager manager = new VirtualQueueOffsetManager();
            String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
            String offsetGroupCollection = com.redismq.common.constant.RedisMQConstant.getOffsetGroupCollection(groupId);

            // 从Redis获取该虚拟队列的已提交偏移量
            Long vqOffset = redisMQClientUtil.getQueueGroupOffset(offsetGroupCollection, vQueueName);
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(queueName);

            // ❌ 暂时禁用: 新消费者组的LATEST跳转会导致startScore过大，拉不到消息
            // 原因: queueMaxOffset是逻辑队列的全局offset计数器，虚拟队列的实际offset可能小于这个值
            // 导致查询条件 score >= (queueMaxOffset + 1) 匹配不到任何消息
            // TODO: 后续需要实现 getVirtualQueueMaxOffset 方法获取虚拟队列的实际最大offset
            // OffsetEnum newGroupOffset = GlobalConfigCache.CONSUMER_CONFIG.getNewGroupOffset();
            // if (newGroupOffset.equals(OffsetEnum.LATEST) && (vqOffset == null || vqOffset == 0L)) {
            //     vqOffset = queueMaxOffset;
            //     log.info("redis-mq New consumer group using LATEST offset: vQueue={}, offset={}",
            //             vQueueName, vqOffset);
            // }

            // ❌ 删除: autoOffsetConsume=LATEST会导致每次初始化都无条件跳转，覆盖已有offset
            // 这会导致消费者重启后跳过未消费的消息，且同样存在startScore过大的问题
            // OffsetEnum autoOffsetConsume = GlobalConfigCache.CONSUMER_CONFIG.getAutoOffsetConsume();
            // if (autoOffsetConsume.equals(OffsetEnum.LATEST)) {
            //     vqOffset = queueMaxOffset;
            //     log.info("redis-mq Consumer group auto reset to LATEST offset: vQueue={}, offset={}",
            //             vQueueName, vqOffset);
            // }

            manager.committedOffset = vqOffset != null ? vqOffset : 0L;

            // 计算是否需要追赶offset
            long diff = queueMaxOffset - manager.committedOffset;
            manager.pullOffsetLow = diff > GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax();

            log.info("redis-mq Initialize VQ offset manager: vQueue={}, committedOffset={}, queueMaxOffset={}, pullOffsetLow={}",
                    vQueueName, manager.committedOffset, queueMaxOffset, manager.pullOffsetLow);
            return manager;
        });
    }


    /**
     * 拉取队列消息
     *
     * <p>主流程：</p>
     * <ol>
     *   <li>计算可用槽位：处理队列满、延时队列、窗口限制</li>
     *   <li>拉取消息：先从持久化存储，再从Redis</li>
     *   <li>过滤重复：防止重复消费</li>
     *   <li>处理空消息：等待或获取延时时间</li>
     *   <li>构建Callable：创建消费任务</li>
     *   <li>更新偏移量：记录最新偏移量</li>
     *   <li>提交线程池：异步执行消费任务</li>
     * </ol>
     *
     * @param vQueueName 虚拟队列名称
     * @return 延时时间集合（延时队列使用）
     */
    public Set<Long> pull(String vQueueName) {
        Set<Long> delayTimes = new LinkedHashSet<>();
        List<Future<Message>> futures = new ArrayList<>();
        
        while (isRunning()) {
            try {
                long pullTime = System.currentTimeMillis();
                
                // 0. 清理已完成的Future并标记消息状态（非延时队列）
                if (!delay) {
                    processCompletedFutures(vQueueName, futures);
                    // 全局提交连续偏移量（跨所有虚拟队列）
                    commitContinuousOffset(vQueueName);
                }
                
                // 1. 计算可用槽位（处理队列满、延时队列等待、窗口限制）
                int availableSlots = calculateAvailableSlots(vQueueName, futures);
                
                // 2. 拉取消息（先从持久化存储，再从Redis）
                List<Message> messages = pullMessages(vQueueName, pullTime, availableSlots);

                // 3. 过滤重复消息（防止重复消费）
                messages = filterDuplicateMessages(vQueueName, messages);
                
                // 4. 处理空消息场景
                if (CollectionUtils.isEmpty(messages)) {
                    if (!handleEmptyMessages(vQueueName, futures, delayTimes, pullTime)) {
                        break;
                    }
                    continue;
                }
                
                // 5. 构建Callable列表
                List<RedisListenerCallable> callables = buildCallables(vQueueName, messages);
                
                // 6. 提交到线程池
                if (!submitToThreadPool(vQueueName, callables, futures)) {
                    continue;
                }
                
            } catch (Throwable e) {
                handlePullError(e);
            }
        }
        
        return delayTimes;
    }
    
    /**
     * 计算可用的拉取槽位数量
     *
     * <p>该方法处理以下场景：</p>
     * <ul>
     *   <li>队列满：等待所有任务完成</li>
     *   <li>延时队列：必须等待所有任务完成</li>
     *   <li>窗口满：达到MAX_OFFSET_WINDOW_SIZE时等待</li>
     *   <li>窗口告警：达到80%时记录告警日志</li>
     * </ul>
     *
     * @param vQueueName 虚拟队列名称
     * @param futures 待完成的Future列表
     * @return 可用的拉取槽位数量
     */
    private int calculateAvailableSlots(String vQueueName, List<Future<Message>> futures) {
        int availableSlots = super.maxConcurrency - futures.size();
        
        // 场景1：消费队列已满，等待所有任务消费完成
        if (availableSlots <= 0) {
            return waitConsumeAndCommit(vQueueName, futures, GLOBAL_CONFIG.getTaskTimeout(), true);
        }
        
        // 场景2：延时队列必须等待执行完成后才能获取下一次的消息
        if (delay && !futures.isEmpty()) {
            return waitConsumeAndCommit(vQueueName, futures, GLOBAL_CONFIG.getTaskTimeout(), true);
        }
        
        // 场景3：检查偏移量窗口是否已满（仅普通队列）
        if (!delay) {
            VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
            int windowSize = vqManager.offsetWindow.size();
            if (windowSize >= MAX_OFFSET_WINDOW_SIZE) {
                log.warn("redis-mq Offset window is FULL, waiting for completion. vQueue: {}, size: {}",
                        vQueueName, windowSize);
                return waitConsumeAndCommit(vQueueName, futures, GLOBAL_CONFIG.getTaskTimeout(), true);
            } else if (windowSize >= OFFSET_WINDOW_WARN_SIZE) {
                // 达到告警阈值，记录warn日志
                log.warn("redis-mq Offset window size approaching limit. vQueue: {}, size: {}, limit: {}",
                        vQueueName, windowSize, MAX_OFFSET_WINDOW_SIZE);
            }
        }
        
        return availableSlots;
    }
    
    /**
     * 拉取消息（先从持久化存储，再从Redis）
     *
     * @param vQueueName 虚拟队列名称
     * @param pullTime 拉取时间戳
     * @param pullSize 拉取数量
     * @return 消息列表
     */
    private List<Message> pullMessages(String vQueueName, long pullTime, int pullSize) {
        // 先获取偏移量落后的group的持久化的message
        List<Message> messages = getOffsetLowStoreMessage(vQueueName);
        
        // 从redis中获取消息
        if (CollectionUtils.isEmpty(messages)) {
            long startScore;
            if (delay) {
                // 延时队列需要从0开始检查，因为要查找所有已到期的消息
                startScore = 0;
            } else {
                // 普通队列：使用虚拟队列offsetWindow中的最大offset+1作为起点，跳过正在处理的消息，拉取新消息
                VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
                startScore = vqManager.offsetWindow.isEmpty() ?
                        (vqManager.committedOffset + 1) : (vqManager.offsetWindow.lastKey() + 1);
            }
            messages = redisMQClientUtil.pullMessage(vQueueName, startScore, pullTime, 0, pullSize);
        }
        
        return messages;
    }
    
    /**
     * 过滤重复消息（防止重复消费）
     *
     * @param vQueueName 虚拟队列名称
     * @param messages 原始消息列表
     * @return 过滤后的消息列表
     */
    private List<Message> filterDuplicateMessages(String vQueueName, List<Message> messages) {
        if (CollectionUtils.isEmpty(messages) || delay) {
            return messages;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        return messages.stream()
                .filter(msg -> msg != null && !vqManager.pullingMessageIds.contains(msg.getId()))
                .collect(Collectors.toList());
    }
    
    /**
     * 处理没有消息的情况
     *
     * @param vQueueName 虚拟队列名称
     * @param futures 待完成的Future列表
     * @param delayTimes 延时时间集合（延时队列使用）
     * @param pullTime 拉取时间戳
     * @return true表示继续循环，false表示退出循环
     */
    private boolean handleEmptyMessages(String vQueueName, List<Future<Message>> futures,
            Set<Long> delayTimes, long pullTime) {
        // 响应中断
        if (!isRunning()) {
            return false;
        }

        // 消息已经拉不到了。如果消费未完成 等待1秒钟消费完成，如果1秒没有消费完。
        // 再继续拉取消息，因为有可能有其他新的消息进来。
        if (!futures.isEmpty()) {
            waitConsumeAndCommit(vQueueName, futures, GLOBAL_CONFIG.getTaskWaitTime(), false);
            return true;
        }

        // 延时队列：获取头部数据的延时时间，加入时间轮
        if (delay) {
            List<Pair<Message, Double>> pairs = redisMQClientUtil
                    .pullMessageByTimeWithScope(vQueueName, pullTime, 0, GLOBAL_CONFIG.delayQueuePullSize);
            pairs.forEach(pair -> delayTimes.add(pair.getValue().longValue()));
        }

        // ✅ 修复: 如果offsetWindow不为空,说明还有消息未提交offset,需要继续循环等待
        // 场景: 消息已经拉取并处理中,但还没提交offset,此时不能退出循环
        // 否则会导致消息消费完但offset未提交,下次重启会重复消费
        if (!delay) {
            VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
            if (!vqManager.offsetWindow.isEmpty()) {
                // ✅ 关键修复: 即使futures为空,也要尝试处理已完成的消息
                // 因为futures可能在其他地方被清空了,但消息实际已经执行完成
                // 这里传入空列表只是为了触发检查,实际我们需要检查offsetWindow中的消息状态
                processCompletedFutures(vQueueName, futures);

                // 尝试提交offset
                commitContinuousOffset(vQueueName);

                // 如果提交后offsetWindow还不为空,继续等待
                if (!vqManager.offsetWindow.isEmpty()) {
                    log.info("redis-mq No new messages, but offsetWindow not empty. vQueue: {}, window size: {}, waiting for completion...",
                            vQueueName, vqManager.offsetWindow.size());
                    try {
                        Thread.sleep(GLOBAL_CONFIG.getTaskWaitTime());  // 等待1秒后继续检查
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return true;  // 继续循环
                }
            }
        }

        return false;
    }
    
    /**
     * 构建Callable列表
     *
     * @param vQueueName 虚拟队列名称
     * @param messages 消息列表
     * @return RedisListenerCallable列表
     */
    private List<RedisListenerCallable> buildCallables(String vQueueName, List<Message> messages) {
        List<RedisListenerCallable> callableInvokes = new ArrayList<>();
        
        for (Message message : messages) {
            if (!isRunning()) {
                break;
            }
            if (message == null) {
                continue;
            }
            
            try {
                // 手动ack模式
                if (AckMode.MAUAL.equals(ackMode)) {
                    // 手动ACK，无需框架处理
                }
                
                String id = super.getRunableKey(message.getTag());
                RedisListenerCallable callable = super.getRedisListenerCallable(id, message);
                if (callable == null) {
                    // 如果是框架中的异常,说明异常是不可修复的.删除异常的消息
                    redisMQClientUtil.removeMessage(vQueueName, message.getId());
                    log.error("RedisMqException not found queue or tag, removeMessage: {}",
                            RedisMQStringMapper.toJsonStr(message));
                    continue;
                }
                
                callableInvokes.add(callable);

                // 将消息添加到偏移量窗口和正在拉取集合（仅普通队列）
                trackMessageInWindow(vQueueName, message);
                
            } catch (Throwable e) {
                if (isRunning()) {
                    log.error("redisMQ listener container error", e);
                }
            }
        }
        
        return callableInvokes;
    }
    
    /**
     * 追踪消息到窗口（仅普通队列）
     *
     * @param vQueueName 虚拟队列名称
     * @param message 消息对象
     */
    private void trackMessageInWindow(String vQueueName, Message message) {
        if (!delay) {
            VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
            vqManager.pullingMessageIds.add(message.getId());
            vqManager.offsetWindow.put(message.getOffset(), new MessageStatus(message.getId(), message));
        }
    }
    
    /**
     * 提交Callable到线程池
     *
     * @param vQueueName 虚拟队列名称
     * @param callables Callable列表
     * @param futures Future列表（用于收集返回的Future）
     * @return true表示提交成功，false表示列表为空
     */
    private boolean submitToThreadPool(String vQueueName, List<RedisListenerCallable> callables,
            List<Future<Message>> futures) {
        if (CollectionUtils.isEmpty(callables)) {
            if (isRunning()) {
                log.error("redisMQ callableInvokes isEmpty queueName:{}", vQueueName);
            }
            return false;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);

        for (RedisListenerCallable callable : callables) {
            // ✅ 包装Callable,在执行完成后自动标记finished
            Callable<Message> wrappedCallable = () -> {
                Message result = null;
                try {
                    result = callable.call();
                    return result;
                } finally {
                    // 无论成功或失败,都标记finished,避免阻塞offset提交
                    if (!delay && result != null) {
                        vqManager.pullingMessageIds.remove(result.getId());
                        MessageStatus status = vqManager.offsetWindow.get(result.getOffset());
                        if (status != null) {
                            status.finished = true;
                            if (log.isDebugEnabled()) {
                                log.debug("redis-mq Message marked as finished. vQueue: {}, offset: {}, messageId: {}",
                                        vQueueName, result.getOffset(), result.getId());
                            }
                        }
                    }
                }
            };

            Future<Message> submit = work.submit(wrappedCallable);
            futures.add(submit);
        }

        return true;
    }
    
    /**
     * 处理拉取错误
     *
     * @param e 异常对象
     */
    private void handlePullError(Throwable e) {
        if (!isRunning()) {
            return;
        }
        
        log.error("redisMQ pop error", e);
        
        // 检查是否是Redis数据类型错误
        if (e.getMessage() != null && e.getMessage().contains("WRONGTYPE Operation against a key holding the wrong kind of value")) {
            log.error("redisMQ [ERROR] queue not is zset type. cancel pop");
            stop();
            return;
        }
        
        // 发生错误后等待5秒再继续
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 处理已完成的Future，标记消息状态并提交偏移量
     *
     * 该方法在每次pull循环中被调用，确保已完成的消息能够及时标记为finished
     * 并触发偏移量提交，避免消息消费了但没有ACK的问题
     *
     * @param vQueueName 虚拟队列名称
     * @param futures Future列表
     */
    private void processCompletedFutures(String vQueueName, List<Future<Message>> futures) {
        if (futures.isEmpty()) {
            return;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        Iterator<Future<Message>> iterator = futures.iterator();
        boolean hasCompleted = false;
        int completedCount = 0;

        while (iterator.hasNext()) {
            Future<Message> future = iterator.next();

            // 检查Future是否已完成
            if (future.isDone()) {
                try {
                    // 获取已完成的消息（不会阻塞）
                    Message msg = future.get();

                    // 标记消息为已完成
                    vqManager.pullingMessageIds.remove(msg.getId());
                    MessageStatus status = vqManager.offsetWindow.get(msg.getOffset());
                    if (status != null) {
                        status.finished = true;
                        hasCompleted = true;
                        completedCount++;

                        if (log.isDebugEnabled()) {
                            log.debug("redis-mq Message completed. vQueue: {}, messageId: {}, offset: {}",
                                    vQueueName, msg.getId(), msg.getOffset());
                        }
                    } else {
                        log.warn("redis-mq Message completed but not found in offsetWindow. " +
                                        "vQueue: {}, messageId: {}, offset: {}",
                                vQueueName, msg.getId(), msg.getOffset());
                    }

                    // 从futures列表中移除
                    iterator.remove();

                } catch (Exception e) {
                    // 任务执行异常，需要清理状态并标记为完成，避免阻塞后续消息
                    log.error("redis-mq Failed to get completed message result", e);
                    log.warn("redis-mq Unable to retrieve message from failed future, " +
                            "this may cause offset window blockage. Exception: {}", e.getMessage());

                    // 从futures列表中移除
                    iterator.remove();
                }
            }
        }

        // 记录完成情况（偏移量提交将在主循环中全局执行）
        if (hasCompleted) {
            log.info("redis-mq processCompletedFutures: vQueue: {}, completed: {}, remaining futures: {}, offsetWindow size: {}",
                    vQueueName, completedCount, futures.size(), vqManager.offsetWindow.size());
        }
    }
    
    /**
     * 获取偏移量落后的队列消息（从MySQL持久化存储拉取）
     *
     * 适用场景：
     * 1. 消费者组长时间下线，Redis中的消息已被删除，但MySQL中还有历史消息
     * 2. 消费进度严重落后，需要追赶
     *
     * @param vQueueName 虚拟队列名
     * @return 历史消息列表，如果不需要追赶则返回null
     */
    private List<Message> getOffsetLowStoreMessage(String vQueueName) {
        if (remotingClient == null) {
            log.warn("remotingClient not register not getOffsetLowStoreMessage please open spring.redismq.netty-config.client.enable=true");
            return null;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        if (!vqManager.pullOffsetLow) {
            return null;
        }

        // 新消费者组处理
        OffsetEnum newGroupOffset = GlobalConfigCache.CONSUMER_CONFIG.getNewGroupOffset();
        if (newGroupOffset.equals(OffsetEnum.LATEST)) {
            vqManager.pullOffsetLow = false;
            return null;
        } else {
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(queueName);
            // 计算需要追赶的范围
            long diff = queueMaxOffset - vqManager.committedOffset;

            if (diff <= 0) {
                // 已经追上或超过最小offset，不需要追赶
                vqManager.pullOffsetLow = false;
                return null;
            }

            if (diff <= GlobalConfigCache.CONSUMER_CONFIG.getGroupOffsetLowMax()) {
                // 差距不大，从Redis拉取就够了
                log.debug("Offset diff is small: {}, skip catchup", diff);
                vqManager.pullOffsetLow = false;
                return null;
            }


            if (!GlobalConfigCache.NETTY_CONFIG.getServer().isEnable()) {
                log.warn("Offset diff too large: {}, but netty server not enabled. Skip catchup.", diff);
                vqManager.pullOffsetLow = false;
                return null;
            }
            // 差距很大，需要从MySQL追赶
            log.info("Start offset catchup. vQueueName: {}, groupId: {}, committedOffset: {}, queueMaxOffset: {}, diff: {}",
                    vQueueName, GlobalConfigCache.CONSUMER_CONFIG.getGroupId(), vqManager.committedOffset, queueMaxOffset, diff);

            GroupOffsetQeueryMessageDTO offsetDTO = new GroupOffsetQeueryMessageDTO();
            offsetDTO.setOffset(vqManager.committedOffset);
            offsetDTO.setVQueue(vQueueName);
            offsetDTO.setLastOffset(queueMaxOffset);  // 追赶到最小offset即可

            String object = (String) remotingClient.sendSync(offsetDTO, MessageType.GET_QUEUE_MESSAGE_BY_OFFSET);
            if (object == null) {
                return null;
            }

            List<Map> list = RedisMQStringMapper.toList(object, Map.class);
            List<Message> messages = new ArrayList<>();

            if (!CollectionUtils.isEmpty(list)) {
                for (Map map : list) {
                    String jsonStr = RedisMQStringMapper.toJsonStr(map);
                    Message msg = RedisMQStringMapper.toBean(jsonStr, Message.class);
                    messages.add(msg);
                }
                log.info("Catchup messages from MySQL. groupId: {}, committedOffset: {}, messageIds: {}",
                        GlobalConfigCache.CONSUMER_CONFIG.getGroupId(), vqManager.committedOffset,
                        messages.stream().map(Message::getId).collect(Collectors.toList()));
            } else {
                // 追赶失败，可能消息已被删除
                log.warn("Catchup failed, messages not found in MySQL. Skip to queueMaxOffset: {}. vQueueName: {}",
                        queueMaxOffset, vQueueName);
                vqManager.pullOffsetLow = false;
            }
            return messages;
        }
    }
    
    /**
     * 等待消费任务完成并提交偏移量
     *
     * @param vQueueName 虚拟队列名称
     * @param futures 待完成的Future列表
     * @param milliseconds 等待超时时间（毫秒）
     * @param timeoutDrop 是否在超时后丢弃所有未完成的Future
     * @return 可用的拉取槽位数量
     */
    private int waitConsumeAndCommit(String vQueueName, List<Future<Message>> futures, long milliseconds, boolean timeoutDrop) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        List<Message> completedMessages = new ArrayList<>();

        for (Future<Message> future : futures) {
            try {
                Message msg = future.get(milliseconds, TimeUnit.MILLISECONDS);
                completedMessages.add(msg);

                // 对于非延时队列，标记消息为已完成
                if (!delay) {
                    vqManager.pullingMessageIds.remove(msg.getId());
                    MessageStatus status = vqManager.offsetWindow.get(msg.getOffset());
                    if (status != null) {
                        status.finished = true;
                    }
                }
            } catch (java.util.concurrent.TimeoutException e) {
                if (log.isDebugEnabled()) {
                    log.debug("redisMQ message consume timeout, will continue in background. timeout: {}ms", milliseconds);
                }
            } catch (java.util.concurrent.CancellationException e) {
                // 任务被取消
                log.warn("redisMQ message consume task cancelled", e);
            } catch (java.util.concurrent.ExecutionException e) {
                // 任务执行过程中抛出异常
                log.error("redisMQ message consume execution error", e.getCause() != null ? e.getCause() : e);
            } catch (InterruptedException e) {
                // 线程被中断
                Thread.currentThread().interrupt();
                log.error("redisMQ message consume interrupted", e);
                break;
            }
        }

        // 根据timeoutDrop决定是否清空futures
        if (timeoutDrop) {
            futures.clear();
        } else {
            futures.removeIf(Future::isDone);
        }

        // 尝试提交偏移量（即使没有完成的消息，也可能有超时需要处理）
        if (!delay) {
            commitContinuousOffset(vQueueName);
        } else if (!completedMessages.isEmpty()) {
            // 延时队列：正常ACK已完成的消息
            ackMessage(vQueueName, completedMessages);
        }

        completedMessages.clear();
        return super.maxConcurrency;
    }
    
    private void ackMessage(String vQueueName, List<Message> messageList) {
        if (!messageList.isEmpty()){
            Long offset = messageList.stream().map(Message::getOffset).max(Long::compareTo).get();
            String msgIds = messageList.stream().map(Message::getId).collect(Collectors.joining(","));
            redisMQClientUtil.ackBatchMessage(vQueueName,msgIds,offset);
        }
    }
    
    /**
     * 提交连续的偏移量，处理超时消息
     *
     * 该方法负责：
     * 1. 检查并处理极端超时的消息（移到死信队列）
     * 2. 查找连续完成的偏移量（虚拟队列级别）
     * 3. 批量ACK并更新committedOffset
     * 4. 清理已完成的消息状态
     *
     * @param vQueueName 虚拟队列名称
     */
    private void commitContinuousOffset(String vQueueName) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);

        if (vqManager.offsetWindow.isEmpty()) {
            log.debug("redis-mq commitContinuousOffset: vQueue={}, offsetWindow is empty, skip", vQueueName);
            return;
        }

        log.info("redis-mq commitContinuousOffset START: vQueue: {}, committedOffset: {}, offsetWindow size: {}, first offset: {}, last offset: {}",
                vQueueName, vqManager.committedOffset, vqManager.offsetWindow.size(),
                vqManager.offsetWindow.firstKey(), vqManager.offsetWindow.lastKey());

        List<Message> dlqMessages = new ArrayList<>();
        List<Long> toRemoveOffsets = new ArrayList<>();
        List<String> toAckMsgIds = new ArrayList<>();
        long newCommittedOffset = vqManager.committedOffset;
        int extremeTimeoutCount = 0;

        // 使用单一同步块处理所有offsetWindow操作，避免并发问题
        synchronized (vqManager.offsetWindow) {
            // 1. 检查并处理极端超时的消息
            for (Map.Entry<Long, MessageStatus> entry : vqManager.offsetWindow.entrySet()) {
                MessageStatus status = entry.getValue();
                if (status == null || status.messageId == null) {
                    log.error("redis-mq Invalid MessageStatus in offsetWindow, offset: {}", entry.getKey());
                    continue;
                }

                if (status.isExtremeTimeout(EXTREME_TIMEOUT)) {
                    extremeTimeoutCount++;
                    log.error("redis-mq EXTREME TIMEOUT DETECTED!!! Message will be moved to DLQ. " +
                                    "vQueue: {}, messageId: {}, offset: {}, timeout: {}ms",
                            vQueueName, status.messageId, entry.getKey(), EXTREME_TIMEOUT);

                    // 加入死信队列列表
                    dlqMessages.add(status.message);

                    // 标记为已完成，允许偏移量前进
                    status.finished = true;

                    // 从正在拉取集合中移除
                    vqManager.pullingMessageIds.remove(status.messageId);
                }
            }

            // 2. 查找连续完成的偏移量（现在offsetWindow已经是虚拟队列级别，无需过滤）
            for (Map.Entry<Long, MessageStatus> entry : vqManager.offsetWindow.entrySet()) {
                Long offset = entry.getKey();
                MessageStatus status = entry.getValue();

                if (status.finished) {
                    // 已完成的消息可以ACK
                    toRemoveOffsets.add(offset);
                    toAckMsgIds.add(status.messageId);
                    // 记录最大的offset
                    if (offset > newCommittedOffset) {
                        newCommittedOffset = offset;
                    }
                } else {
                    // 遇到未完成消息就停止（虚拟队列内保证顺序消费）
                    log.info("redis-mq commitContinuousOffset BREAK: vQueue={}, found unfinished message at offset: {}",
                            vQueueName, offset);
                    break;
                }
            }

            // 3. 清理已提交的偏移量（在同步块内完成）
            for (Long offset : toRemoveOffsets) {
                vqManager.offsetWindow.remove(offset);
            }
        }

        // 4. 将极端超时消息移到死信队列（不需要同步，已经从window中移除）
        if (!dlqMessages.isEmpty()) {
            log.error("redis-mq Extreme timeout summary: vQueue: {}, count: {}, offsets: {}",
                    vQueueName, extremeTimeoutCount,
                    dlqMessages.stream().map(Message::getOffset).collect(Collectors.toList()));

            for (Message dlqMsg : dlqMessages) {
                try {
                    // 移除原消息
                    redisMQClientUtil.removeMessage(vQueueName, dlqMsg.getId());
                    // 添加到死信队列
                    redisMQClientUtil.putDeadQueue(dlqMsg);
                    log.error("redis-mq EXTREME TIMEOUT!!! Message moved to DEAD QUEUE. " +
                                    "vQueue: {}, messageId: {}, offset: {}",
                            vQueueName, dlqMsg.getId(), dlqMsg.getOffset());
                } catch (Exception e) {
                    log.error("redis-mq Failed to move message to dead queue, vQueue: {}, messageId: {}",
                            vQueueName, dlqMsg.getId(), e);
                }
            }
        }

        // 5. 批量ACK连续完成的消息
        if (newCommittedOffset > vqManager.committedOffset && !toAckMsgIds.isEmpty()) {
            try {
                String msgIdStr = String.join(",", toAckMsgIds);
                redisMQClientUtil.ackBatchMessage(vQueueName, msgIdStr, newCommittedOffset);

                log.info("redis-mq Committed continuous offset: vQueue={}, {} -> {}, message count: {}",
                        vQueueName, vqManager.committedOffset, newCommittedOffset, toAckMsgIds.size());

                // 更新已提交的偏移量
                vqManager.committedOffset = newCommittedOffset;
            } catch (Exception e) {
                log.error("redis-mq Failed to ack batch messages, vQueue: {}, offset: {} -> {}",
                        vQueueName, vqManager.committedOffset, newCommittedOffset, e);
            }
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

