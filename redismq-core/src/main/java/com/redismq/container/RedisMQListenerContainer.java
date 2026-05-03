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

import java.util.*;
import java.util.concurrent.*;
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
    /**
     * 全局消费槽位。
     * <p>
     * 设计目的：
     * 1. 拉取线程只有拿到槽位后才允许把消息提交到业务线程池，防止“先拉一堆、再慢慢消费”把内存打满。
     * 2. 槽位在任务真正执行结束后才归还，因此空闲线程一旦出现，下一轮 pull 很快就能再次拉消息。
     * 3. 这样就不再需要对 Future 做串行 get(timeout) 等待，拉取线程不会因为某条慢消息而整体卡住。
     */
    private final Semaphore consumeSlots;
    /**
     * 正在渐进下线的虚拟队列集合。
     * <p>
     * rebalance 释放分片时，不再像旧实现那样立刻 pause + unlock，而是先把虚拟队列标记成 quiescing：
     * 1. 禁止继续为该虚拟队列拉取新消息；
     * 2. 允许已提交的任务把手头工作做完；
     * 3. 等 offsetWindow / inFlight 排空后，pull 循环自然退出，锁再由新 owner 抢占。
     */
    private final Set<String> quiescingVirtualQueues = ConcurrentHashMap.newKeySet();
    private RemotingClient remotingClient;

    /**
     * 虚拟队列偏移量管理器（每个虚拟队列独立管理offset、window、消息ID）
     */
    private static class VirtualQueueOffsetManager {
        /**
         * 该虚拟队列已提交的最大连续偏移量
         */
        volatile long committedOffset = 0;
        /**
         * 偏移量窗口（维护消息完成状态）
         */
        final SortedMap<Long, MessageStatus> offsetWindow = Collections.synchronizedSortedMap(new TreeMap<>());
        /**
         * 正在拉取的消息ID集合（防止重复拉取）
         */
        final Set<String> pullingMessageIds = ConcurrentHashMap.newKeySet();
        /**
         * 是否需要从持久化存储追赶offset
         */
        volatile boolean pullOffsetLow = false;
        /**
         * 当前虚拟队列正在执行或已提交但尚未结束的任务数。
         * <p>
         * 普通队列和延时队列都要依赖这个值：
         * 1. 延时队列要求“上一批完全结束后再拉下一批”；
         * 2. quiesce 时要靠它判断当前虚拟队列是否已经彻底排空。
         */
        final AtomicInteger inFlight = new AtomicInteger();
        /**
         * 延时队列已完成但尚未统一 ACK 的消息。
         * <p>
         * 延时队列没有普通队列那种 offsetWindow 连续提交逻辑，因此任务线程只负责把完成结果回传到这里，
         * 真正的 ACK 仍然放回 pull 线程统一批量提交，避免业务线程直接和 ACK 时序纠缠在一起。
         */
        final ConcurrentLinkedQueue<Message> completedMessages = new ConcurrentLinkedQueue<>();
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
        /**
         * 进入窗口的时间，用来判断极端超时
         */
        final long pullTime;
        /**
         * 真实提交到线程池后的 Future，仅用于极端超时场景尝试 cancel
         */
        volatile Future<Message> future;

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
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        lifeExtensionThread.shutdownNow();
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
        consumeSlots = new Semaphore(getMaxConcurrency());
        // 线程池本身负责真正的业务消费。
        // 这里使用 SynchronousQueue 直接移交任务，避免任务先在队列里堆满而线程池迟迟不扩到 maxConcurrency。
        // 在当前容器里，consumeSlots 已经限制了“飞行中的任务总数 <= maxConcurrency”，
        // 因此不需要再额外缓存一大批待执行任务。
        // 当线程池已经扩满且没有空闲线程时，AbortPolicy 会立刻把背压抛回拉取线程，
        // 而不是像 CallerRunsPolicy 那样让拉取线程反向去执行业务逻辑，导致调度线程被业务消费拖住。
        work = new ThreadPoolExecutor(getConcurrency(), getMaxConcurrency(), 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                createThreadFactory(queue.getQueueName()),
                new ThreadPoolExecutor.AbortPolicy());
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

            // 从Redis获取该虚拟队列的已提交偏移量
            Long vqOffset = redisMQClientUtil.getQueueGroupOffset(groupId, vQueueName);
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(vQueueName);

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

        while (isRunning()) {
            try {
                long pullTime = System.currentTimeMillis();

                // 先处理“已经完成但还没提交”的结果，再决定本轮还能不能继续拉。
                // 普通队列走连续 offset 提交；延时队列走 completedMessages 批量 ACK。
                if (!delay) {
                    commitContinuousOffset(vQueueName);
                } else {
                    drainDelayCompletedMessages(vQueueName);
                }

                // rebalance 释放分片时进入 quiesce，只排空、不拉新。
                if (isQuiescing(vQueueName)) {
                    if (!hasPendingWork(vQueueName)) {
                        break;
                    }
                    sleepForNextCheck();
                    continue;
                }

                int availableSlots = calculateAvailableSlots(vQueueName);
                if (availableSlots <= 0) {
                    sleepForNextCheck();
                    continue;
                }

                List<Message> messages = pullMessages(vQueueName, pullTime, availableSlots);
                messages = filterDuplicateMessages(vQueueName, messages);

                if (CollectionUtils.isEmpty(messages)) {
                    if (!handleEmptyMessages(vQueueName, delayTimes, pullTime)) {
                        break;
                    }
                    continue;
                }

                // 这里仍然按“先构造任务、再统一提交”的方式推进，但是否能真正提交由 consumeSlots 决定。
                // 因此即使某一轮拉到了多条消息，也只会在当前可承受的并发范围内入池。
                List<RedisListenerCallable> callables = buildCallables(vQueueName, messages);
                submitToThreadPool(vQueueName, callables);

            } catch (Throwable e) {
                handlePullError(e);
            }
        }

        if (!delay) {
            commitContinuousOffset(vQueueName);
        } else {
            drainDelayCompletedMessages(vQueueName);
        }
        return delayTimes;
    }

    /**
     * 计算可用的拉取槽位数量
     *
     * <p>该方法处理以下场景：</p>
     * <ul>
     *   <li>全局槽位不足：直接返回0，由 pull 主循环短暂 sleep 后重试</li>
     *   <li>延时队列：必须等待当前批次全部完成后才能继续拉</li>
     *   <li>窗口满：达到MAX_OFFSET_WINDOW_SIZE时不再拉新消息</li>
     *   <li>窗口告警：达到80%时记录告警日志</li>
     * </ul>
     *
     * @param vQueueName 虚拟队列名称
     * @return 可用的拉取槽位数量
     */
    private int calculateAvailableSlots(String vQueueName) {
        int availableSlots = consumeSlots.availablePermits();
        if (availableSlots <= 0) {
            return 0;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        if (delay && vqManager.inFlight.get() > 0) {
            return 0;
        }

        if (!delay) {
            int windowSize = vqManager.offsetWindow.size();
            if (windowSize >= MAX_OFFSET_WINDOW_SIZE) {
                log.warn("redis-mq Offset window is FULL, waiting for completion. vQueue: {}, size: {}",
                        vQueueName, windowSize);
                return 0;
            } else if (windowSize >= OFFSET_WINDOW_WARN_SIZE) {
                log.warn("redis-mq Offset window size approaching limit. vQueue: {}, size: {}, limit: {}",
                        vQueueName, windowSize, MAX_OFFSET_WINDOW_SIZE);
            }
            availableSlots = Math.min(availableSlots, MAX_OFFSET_WINDOW_SIZE - windowSize);
        }

        return availableSlots;
    }

    /**
     * 拉取消息（先从持久化存储，再从Redis）
     *
     * @param vQueueName 虚拟队列名称
     * @param pullTime   拉取时间戳
     * @param pullSize   拉取数量
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
     * @param messages   原始消息列表
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
     * @param delayTimes 延时时间集合（延时队列使用）
     * @param pullTime   拉取时间戳
     * @return true表示继续循环，false表示退出循环
     */
    private boolean handleEmptyMessages(String vQueueName, Set<Long> delayTimes, long pullTime) {
        if (!isRunning()) {
            return false;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);

        if (delay) {
            // 延时队列要求“当前批次完全结束后再继续拉下一批”，否则 ACK 与调度时间会互相穿插。
            if (vqManager.inFlight.get() > 0 || !vqManager.completedMessages.isEmpty()) {
                drainDelayCompletedMessages(vQueueName);
                sleepForNextCheck();
                return true;
            }
            if (isQuiescing(vQueueName)) {
                return false;
            }
            List<Pair<Message, Double>> pairs = redisMQClientUtil
                    .pullMessageByTimeWithScope(vQueueName, pullTime, 0, GLOBAL_CONFIG.delayQueuePullSize);
            pairs.forEach(pair -> delayTimes.add(pair.getValue().longValue()));
            return false;
        }

        if (hasPendingWork(vQueueName)) {
            // 普通队列没有新消息时，仍要继续盯住窗口里的存量任务，直到连续 offset 被推进完。
            commitContinuousOffset(vQueueName);
            sleepForNextCheck();
            return true;
        }

        return false;
    }

    /**
     * 构建Callable列表
     *
     * @param vQueueName 虚拟队列名称
     * @param messages   消息列表
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
     * @param message    消息对象
     */
    private MessageStatus trackMessageInWindow(String vQueueName, Message message) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        // 一定要先把消息登记到窗口，再把任务交给线程池。
        // 这样即使任务刚提交就极速完成，commit 线程也能看到这条 offset 的完整状态，不会出现“消息已经跑完，
        // 但窗口里根本没有记录，导致提交链路断档”的问题。
        MessageStatus status = new MessageStatus(message.getId(), message);
        vqManager.pullingMessageIds.add(message.getId());
        vqManager.offsetWindow.put(message.getOffset(), status);
        return status;
    }

    /**
     * 提交Callable到线程池
     *
     * @param vQueueName 虚拟队列名称
     * @param callables  Callable列表
     * @return true表示提交成功，false表示列表为空
     */
    private boolean submitToThreadPool(String vQueueName, List<RedisListenerCallable> callables) {
        if (CollectionUtils.isEmpty(callables)) {
            if (isRunning()) {
                log.error("redisMQ callableInvokes isEmpty queueName:{}", vQueueName);
            }
            return false;
        }

        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        boolean submitted = false;

        for (RedisListenerCallable callable : callables) {
            Message message = (Message) callable.getArgs();
            if (message == null) {
                continue;
            }
            // 先占用槽位，再尝试真正 submit。
            // 这样可以保证“系统里正在飞行的消息数”永远不超过 consumeSlots 允许的上限。
            if (!consumeSlots.tryAcquire()) {
                break;
            }
            vqManager.inFlight.incrementAndGet();
            MessageStatus status = null;
            try {
                if (!delay) {
                    status = trackMessageInWindow(vQueueName, message);
                }
                MessageStatus finalStatus = status;
                Callable<Message> wrappedCallable = () -> {
                    Message result = message;
                    try {
                        result = callable.call();
                        return result;
                    } finally {
                        onMessageFinished(vQueueName, result != null ? result : message);
                    }
                };
                Future<Message> future = work.submit(wrappedCallable);
                if (finalStatus != null) {
                    // Future 只用于极端超时时尝试 cancel，正常调度流程不再对它做 get(timeout)。
                    finalStatus.future = future;
                }
                submitted = true;
            } catch (RejectedExecutionException e) {
                // 线程池瞬时满载时，必须把“窗口占位 + 槽位 + inFlight”全部回滚干净，
                // 否则后续会看到虚假的窗口膨胀或并发占满。
                rollbackSubmission(vQueueName, message, status);
                log.warn("redisMQ work thread pool is full, submission rejected. queueName:{}, messageId:{}",
                        vQueueName, message.getId(), e);
                break;
            } catch (RuntimeException e) {
                rollbackSubmission(vQueueName, message, status);
                throw e;
            }
        }

        return submitted;
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

    private void rollbackSubmission(String vQueueName, Message message, MessageStatus status) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        if (status != null) {
            vqManager.offsetWindow.remove(message.getOffset());
            vqManager.pullingMessageIds.remove(message.getId());
        }
        // 只有“已经占掉的资源”才需要在这里对称归还。
        vqManager.inFlight.decrementAndGet();
        consumeSlots.release();
    }

    private void onMessageFinished(String vQueueName, Message message) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        try {
            if (delay) {
                // 延时队列不在业务线程中直接 ACK，而是先转回 pull 线程统一批量处理。
                vqManager.completedMessages.add(message);
                return;
            }
            // 普通队列只做“完成标记”，真正的 offset 推进仍由 commitContinuousOffset 保证连续性。
            vqManager.pullingMessageIds.remove(message.getId());
            MessageStatus status = vqManager.offsetWindow.get(message.getOffset());
            if (status != null) {
                status.finished = true;
            }
        } finally {
            vqManager.inFlight.decrementAndGet();
            consumeSlots.release();
        }
    }

    private void drainDelayCompletedMessages(String vQueueName) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        if (vqManager.completedMessages.isEmpty()) {
            return;
        }
        // completedMessages 是并发队列，这里把当前已完成结果尽量一次性捞出做批量 ACK，
        // 让延时队列的 pull 线程只承担轻量协调角色，不把 ACK 压力散落到每个业务线程。
        List<Message> completedMessages = new ArrayList<>();
        Message message;
        while ((message = vqManager.completedMessages.poll()) != null) {
            completedMessages.add(message);
        }
        ackMessage(vQueueName, completedMessages);
    }

    private boolean hasPendingWork(String vQueueName) {
        VirtualQueueOffsetManager vqManager = getOrCreateVQManager(vQueueName);
        if (vqManager.inFlight.get() > 0) {
            return true;
        }
        if (delay) {
            return !vqManager.completedMessages.isEmpty();
        }
        return !vqManager.offsetWindow.isEmpty();
    }

    private boolean isQuiescing(String vQueueName) {
        return quiescingVirtualQueues.contains(vQueueName);
    }

    private void sleepForNextCheck() {
        // 这里故意只 sleep 一个很短的时间片。
        // 目标不是“长时间阻塞等待”，而是给已完成任务一点提交/归还槽位的机会，
        // 随后立即回来继续尝试拉取，让空闲线程尽快重新吃到消息。
        long sleepMillis = Math.min(GLOBAL_CONFIG.getTaskWaitTime(), 50L);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 获取偏移量落后的队列消息（从MySQL持久化存储拉取）
     * <p>
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
            Long queueMaxOffset = redisMQClientUtil.getQueueMaxOffset(vQueueName);
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

    private void ackMessage(String vQueueName, List<Message> messageList) {
        if (!messageList.isEmpty()) {
            Long offset = messageList.stream().map(Message::getOffset).max(Long::compareTo).get();
            String msgIds = messageList.stream().map(Message::getId).collect(Collectors.joining(","));
            redisMQClientUtil.ackBatchMessage(vQueueName, msgIds, offset);
        }
    }

    /**
     * 提交连续的偏移量，处理超时消息
     * <p>
     * 该方法负责：
     * 1. 检查并处理极端超时的消息（先尝试 cancel，再决定是否移到死信队列）
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
            for (Map.Entry<Long, MessageStatus> entry : vqManager.offsetWindow.entrySet()) {
                MessageStatus status = entry.getValue();
                if (status == null || status.messageId == null) {
                    log.error("redis-mq Invalid MessageStatus in offsetWindow, offset: {}", entry.getKey());
                    continue;
                }

                if (status.isExtremeTimeout(EXTREME_TIMEOUT)) {
                    Future<Message> future = status.future;
                    // 极端超时不再直接强推 offset。
                    // 只有任务已经结束、已经取消，或者 cancel(true) 成功时，才允许把这条消息当成“可安全推进”。
                    boolean safeToMove = future == null || future.isDone() || future.isCancelled();
                    if (!safeToMove && future != null) {
                        safeToMove = future.cancel(true);
                    }
                    if (safeToMove) {
                        extremeTimeoutCount++;
                        log.error("redis-mq EXTREME TIMEOUT DETECTED!!! Message will be moved to DLQ. " +
                                        "vQueue: {}, messageId: {}, offset: {}, timeout: {}ms",
                                vQueueName, status.messageId, entry.getKey(), EXTREME_TIMEOUT);
                        dlqMessages.add(status.message);
                        status.finished = true;
                        vqManager.pullingMessageIds.remove(status.messageId);
                    } else {
                        log.error("redis-mq EXTREME TIMEOUT DETECTED but task could not be cancelled. " +
                                        "Keep waiting. vQueue: {}, messageId: {}, offset: {}",
                                vQueueName, status.messageId, entry.getKey());
                    }
                }
            }

            for (Map.Entry<Long, MessageStatus> entry : vqManager.offsetWindow.entrySet()) {
                Long offset = entry.getKey();
                MessageStatus status = entry.getValue();

                if (status.finished) {
                    toRemoveOffsets.add(offset);
                    toAckMsgIds.add(status.messageId);
                    if (offset > newCommittedOffset) {
                        newCommittedOffset = offset;
                    }
                } else {
                    log.info("redis-mq commitContinuousOffset BREAK: vQueue={}, found unfinished message at offset: {}",
                            vQueueName, offset);
                    break;
                }
            }

            for (Long offset : toRemoveOffsets) {
                vqManager.offsetWindow.remove(offset);
            }
        }

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

    public void quiesceVirtualQueue(String virtualQueue) {
        // quiesce 的语义是“停止接新活，但不要粗暴中断正在执行的活”。
        quiescingVirtualQueues.add(virtualQueue);
        delayTimeoutTaskManager.quiesce(virtualQueue);
    }

    public void activateVirtualQueue(String virtualQueue) {
        // 重新获得分片时恢复拉取资格。
        quiescingVirtualQueues.remove(virtualQueue);
    }

    public boolean isVirtualQueueDrained(String virtualQueue) {
        return !hasPendingWork(virtualQueue);
    }

    /**
     * 开始拉取消息的任务。同一个时间一个虚拟队列只允许一个服务的一个线程进行拉取操作
     *
     * @param virtualQueue 虚拟队列
     * @param startTime    开始时间
     */
    public void start(String virtualQueue, Long startTime) {

        running();
        // 同一个虚拟队列重新分配到当前节点时，需要先清掉 quiesce 标记，
        // 否则新的 DelayTimeoutTask 即使被推入，也会在 pull 主循环里立刻退出。
        activateVirtualQueue(virtualQueue);

        //为空说明当前能获取到数据
        DelayTimeoutTask timeoutTask = delayTimeoutTaskManager
                .computeIfAbsent(virtualQueue, task -> new DelayTimeoutTask(virtualQueue, redisMQClientUtil) {
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
