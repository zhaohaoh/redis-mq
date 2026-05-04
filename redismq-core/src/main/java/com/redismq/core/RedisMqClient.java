package com.redismq.core;


import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.pojo.Client;
import com.redismq.common.pojo.PushMessage;
import com.redismq.common.pojo.Queue;
import com.redismq.container.RedisMQListenerContainer;
import com.redismq.id.MsgIDGenerator;
import com.redismq.id.WorkIdGenerator;
import com.redismq.queue.QueueManager;
import com.redismq.rebalance.ClientConfig;
import com.redismq.rebalance.QueueRebalanceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.redismq.common.constant.GlobalConstant.CLIENT_EXPIRE;
import static com.redismq.common.constant.GlobalConstant.CLIENT_RABALANCE_TIME;
import static com.redismq.common.constant.GlobalConstant.CLIENT_REGISTER_TIME;
import static com.redismq.common.constant.GlobalConstant.SPLITE;
import static com.redismq.common.constant.RedisMQConstant.getRebalanceLock;


/**
 * @Author: hzh
 * @Date: 2022/11/4 16:44 RedisMQ客户端  实现负载均衡
 */
public class RedisMqClient {
    
    protected static final Logger log = LoggerFactory.getLogger(RedisMqClient.class);
    /**
     * 注册心跳后等待一个很短的收敛时间，再读取客户端列表做 rebalance。
     *
     * 这个等待不是为了“暂停消费”，而是为了减少刚启动/刚收到 rebalance 广播时，
     * 其他节点心跳尚未写入 Redis 带来的分配抖动。
     */
    private static final long REBALANCE_SETTLE_MILLIS = 200L;
    
    /**
     * 注册线程客户端维持心跳线程
     */
    private final ScheduledThreadPoolExecutor registerThread = new ScheduledThreadPoolExecutor(1);
    
    /**
     * 负载均衡心跳线程
     */
    private final ScheduledThreadPoolExecutor rebalanceThread = new ScheduledThreadPoolExecutor(1);
    
    /**
     * 容器管理者
     */
    private final RedisListenerContainerManager redisListenerContainerManager;
    
    /**
     * redisClient客户端 可以是jedis luccute 和spring
     */
    private final RedisMQClientUtil redisMQStoreUtil;
    
    /**
     * 客户端id
     */
    private final String clientId;
    /**
     * 应用名
     */
    private final String applicationName;
    /**
     * 机器id
     */
    private Integer workId;
    
    /**
     * 负载均衡机制
     */
    private final QueueRebalanceImpl rebalance;
    /**
     * 工作id生成器
     */
    private final WorkIdGenerator workIdGenerator;
    
    /**
     * 容器
     */
    private RedisMessageListenerContainer redisMessageListenerContainer;
    
    /**
     * 是否订阅消息
     */
    private boolean isSub;
    /**
     * 上一次参与 rebalance 的 client 列表签名
     */
    private volatile String lastClientSignature = "";
    /**
     * 订阅/取消订阅必须复用同一个 listener 实例，否则 removeMessageListener 无法精准移除。
     */
    private RedisPullListener pullListener;
    private RedisRebalanceListener rebalanceListener;
    /**
     * rebalance / repush 都会改写当前节点的分片视图并向本地阻塞队列重新投递虚拟队列，
     * 这里串行化它们，避免多个 rebalance 线程交错执行导致重复 quiesce/重复入队。
     */
    private final Object rebalanceMonitor = new Object();
    public RedisMqClient(RedisMQClientUtil redisMQStoreUtil, RedisListenerContainerManager redisListenerContainerManager,
            QueueRebalanceImpl rebalance,String applicationName,WorkIdGenerator workIdGenerator) {
        this.redisMQStoreUtil = redisMQStoreUtil;
        this.clientId = ClientConfig.getLocalAddress() + SPLITE + NanoIdUtils.randomNanoId();
        this.redisListenerContainerManager = redisListenerContainerManager;
        this.rebalance = rebalance;
        this.applicationName = applicationName;
        this.workIdGenerator = workIdGenerator;
    }
    
    public void setRedisMessageListenerContainer(RedisMessageListenerContainer redisMessageListenerContainer) {
        this.redisMessageListenerContainer = redisMessageListenerContainer;
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public RedisListenerContainerManager getRedisListenerContainerManager() {
        return redisListenerContainerManager;
    }
    
    
    public void registerClient() {
        if (workId == null){
            workId = workIdGenerator.getSnowId();
            List<Client> clients = redisMQStoreUtil.getGroupClients();
            List<Integer> workIds = clients.stream().map(Client::getWorkId).collect(Collectors.toList());
            while (workIds.contains(workId)){
                log.error("redis-mq registerClient workId duplicate");
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                workId = workIdGenerator.getSnowId();
            }
            MsgIDGenerator.init(workId);
        }
        
        log.debug("registerClient :{} applicationName:{} workId:{}", clientId, applicationName,workId);
        Client client = new Client();
        client.setClientId(clientId);
        client.setApplicationName(applicationName);
        client.setWorkId(workId);
        client.setGroupId(GlobalConfigCache.CONSUMER_CONFIG.getGroupId());
        client.setQueues(QueueManager.getLocalQueues());
        //注册客户端
        redisMQStoreUtil.registerClient(client);
    }
    
    public void registerGroup(){
        redisMQStoreUtil.registerGroup();
    };
    
    public List<Client> allClient() {
        return redisMQStoreUtil.getGroupClients();
    }
    
    public Long removeExpireClients() {
        // 过期的客户端
        long max = System.currentTimeMillis() - CLIENT_EXPIRE * 1000L;
        return redisMQStoreUtil.removeClient(0, max);
    }
    
    /**
     * 仅保留给显式运维/排障使用。
     *
     * 不能放到默认启动流程里调用，否则新节点启动时会把整个消费组的 client 心跳记录全部删掉，
     * 多节点下会直接破坏正在运行的 rebalance 视图。
     */
    public Long removeAllClient() {
        log.info("redismq removeAllClient");
        return redisMQStoreUtil.removeClient(0, Double.MAX_VALUE);
    }
    
    public void destory() {
        Client client = new Client();
        client.setClientId(clientId);
        client.setApplicationName(applicationName);
        client.setWorkId(workId);
        client.setGroupId(GlobalConfigCache.CONSUMER_CONFIG.getGroupId());
        //停止任务
        redisMQStoreUtil.removeClient(client);
        redisListenerContainerManager.stopAll();
        publishRebalance();
        log.info("redismq client remove currentVirtualQueues:{} ", QueueManager.getCurrentVirtualQueues());
    }
    
    public void start() {
        //移除失效客户端
        removeExpireClients();
        // 注册group
        registerGroup();
        // 先订阅平衡消息,以免平衡的消息没有收到
        rebalanceSubscribe();
        // 订阅服务器消息
        serverSubscribe();
        // 重平衡
        rebalance();
        // 30秒自动注册
        startRegisterClientTask();
        // 20秒自动重平衡
        startRebalanceTask();
        // 启动后补打一轮“过期客户端扫描”。
        // 这个补偿任务用于覆盖一个典型场景：旧节点已经异常下线，但它的 client 心跳还没自然过期，
        // 新节点刚启动时先看到的是一份带脏数据的 client 列表；等过期时间一到，补扫一次即可收敛。
        rebalanceThread.schedule(this::rebalanceTask, CLIENT_EXPIRE + 1L, TimeUnit.SECONDS);
        
        //启动队列监控
        redisListenerContainerManager.startRedisListener();
        //启动延时队列监控
        redisListenerContainerManager.startDelayRedisListener();
        // 启动成功
        log.info("RedisMQ Start Success  \nGroupId:{} \nQueues:{}",GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),QueueManager.getLocalQueues());
    }
    
    private void serverSubscribe() {
        redisMessageListenerContainer.addMessageListener(new RedisServerListener(),
                new ChannelTopic(RedisMQConstant.getServerTopic()));
    }
    
    
    // 多个服务应该只有一个执行重平衡
    public void rebalanceTask() {
        String lockKey = getRebalanceLock();
        Boolean success = redisMQStoreUtil.lock(lockKey, Duration.ofSeconds(CLIENT_RABALANCE_TIME));
        if (success != null && success) {
            Long count = removeExpireClients();
            List<Client> clients = allClient();
            String currentClientSignature = buildClientSignature(clients);
            boolean clientChanged = !currentClientSignature.equals(lastClientSignature);
            if ((count != null && count > 0) || clientChanged) {
                log.info("doRebalance removeExpireClients count=:{}", count);
                log.info("doRebalance clientChanged:{} clientSignature:{}", clientChanged, currentClientSignature);
                rebalance();
            }
        }
    }
    
    /**
     * 平衡
     */
    public void rebalance() {
        // 广播 rebalance 事件，让其他节点也刷新自己的分片视图。
        // 新实现不再要求其他节点立刻 pauseAll + unlock，而是让每个节点自行进入 quiesce 收敛。
        doRebalance(true);
    }
    
    
    /**
     * 执行本地 rebalance。
     * 核心步骤：
     * 1. 先刷新本节点心跳，确保自己在 client 列表中可见；
     * 2. 记录 rebalance 前的分片视图；
     * 3. 等待极短收敛时间，让其他新注册节点也进入视图；
     * 4. 计算新的分片；
     * 5. 只对“释放的虚拟队列”做 quiesce，对“新获得的虚拟队列”重新投递拉取任务。
     * 这样做的关键点是：不再粗暴地 pauseAll + unlock，避免已在执行中的消息被其他节点抢走而重复消费。
     */
    public void doRebalance() {
        doRebalance(false);
    }

    private void doRebalance(boolean publish) {
        synchronized (rebalanceMonitor) {
            registerClient();
            if (publish) {
                // 先让当前节点写入 Redis 再通知其他节点重算
                publishRebalance();
            }
            Map<String, List<String>> previousAssignments = snapshotCurrentAssignments();
            try {
                Thread.sleep(REBALANCE_SETTLE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            List<Client> clients = allClient();
            lastClientSignature = buildClientSignature(clients);
            rebalance.rebalance(clients, clientId);
            applyAssignments(previousAssignments, snapshotCurrentAssignments());
        }
    }
    
    private void publishRebalance() {
        redisMQStoreUtil.publishRebalance(clientId);
    }

    private String buildClientSignature(List<Client> clients) {
        if (CollectionUtils.isEmpty(clients)) {
            return "";
        }
        String groupId = GlobalConfigCache.CONSUMER_CONFIG.getGroupId();
        return clients.stream()
                .filter(client -> groupId.equals(client.getGroupId()))
                .map(client -> client.getClientId() + ":" + buildQueueSignature(client.getQueues()))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    private String buildQueueSignature(List<String> queues) {
        if (CollectionUtils.isEmpty(queues)) {
            return "";
        }
        return queues.stream().sorted().collect(Collectors.joining(","));
    }
    
    
    /**
     * 重平衡时对任务重新进行拉取
     */
    public void repush() {
        synchronized (rebalanceMonitor) {
            // repush 用“空的 previousAssignments”来驱动当前已分配的所有虚拟队列重新入本地调度队列，
            // 常用于节点重启后的本地恢复，不需要再次计算 rebalance。
            applyAssignments(new LinkedHashMap<>(), snapshotCurrentAssignments());
        }
    }
    
    /**
     * 监听队列消息的订阅。
     *
     * 这里必须复用 pullListener：
     * 1. add / remove 要操作同一个 listener 实例；
     * 2. 只有当前节点手里至少有一个虚拟队列分片时才需要保留订阅；
     * 3. 没有分片时及时取消订阅，避免无意义地接收广播后再丢弃。
     */
    public synchronized void subscribe() {
        if (pullListener == null) {
            pullListener = new RedisPullListener(this);
        }
        if (!isSub) {
            redisMessageListenerContainer.addMessageListener(pullListener,
                    new ChannelTopic(RedisMQConstant.getTopic()));
            isSub = true;
        }
    }
    
    /**
     * 取消监听队列消息的订阅。
     */
    public synchronized void unSubscribe() {
        if (isSub) {
            redisMessageListenerContainer.removeMessageListener(pullListener,
                    new ChannelTopic(RedisMQConstant.getTopic()));
            isSub = false;
        }
    }
    
    /**
     * 负载均衡订阅。
     * rebalance topic 在客户端生命周期里应该一直存在，因此这里同样复用 listener，
     * 但不做频繁 add/remove。
     */
    public void rebalanceSubscribe() {
        if (rebalanceListener == null) {
            rebalanceListener = new RedisRebalanceListener(this);
        }
        redisMessageListenerContainer.addMessageListener(rebalanceListener,
                new ChannelTopic(RedisMQConstant.getRebalanceTopic(GlobalConfigCache.CONSUMER_CONFIG.getGroupId())));
    }
    
    /**
     * 开始注册客户任务   心跳任务
     */
    public void startRegisterClientTask() {
        registerThread.scheduleAtFixedRate(this::registerClient, CLIENT_REGISTER_TIME, CLIENT_REGISTER_TIME,
                TimeUnit.SECONDS);
    }
    
    /**
     * 开始负载均衡任务
     */
    public void startRebalanceTask() {
        rebalanceThread.scheduleAtFixedRate(this::rebalanceTask, CLIENT_RABALANCE_TIME, CLIENT_RABALANCE_TIME,
                TimeUnit.SECONDS);
    }
    
    /**
     * 队列寄存器
     *
     * @param queue 队列
     */
    public Queue registerQueue(Queue queue) {
        Set<Queue> allQueue = getAllQueue();
        
        allQueue.stream().filter(redisQueue -> redisQueue.getQueueName().equals(queue.getQueueName()))
                .forEach(redisMQStoreUtil::removeQueue);
        redisMQStoreUtil.registerQueue(queue);
        // 队列也存储group。用来记录offset
        List<String> localVirtualQueues = QueueManager.getLocalVirtualQueues(queue.getQueueName());
        for (String localVirtualQueue : localVirtualQueues) {
            redisMQStoreUtil.registerQueueGroup(localVirtualQueue);
        }
        return queue;
    }
    
    /**
     * 获取所有队列
     *
     * @return {@link Set}<{@link Queue}>
     */
    public Set<Queue> getAllQueue() {
        return redisMQStoreUtil.getQueueList();
    }

    private Map<String, List<String>> snapshotCurrentAssignments() {
        Map<String, List<String>> snapshot = new LinkedHashMap<>();
        QueueManager.getCurrentVirtualQueues()
                .forEach((queue, vQueues) -> snapshot.put(queue,
                        vQueues == null ? new ArrayList<>() : new ArrayList<>(vQueues)));
        return snapshot;
    }

    /**
     * 把 rebalance 结果真正应用到当前节点。
     * 处理顺序非常重要：
     * 1. 先找出 released 分片并 quiesce，立刻阻止这些分片继续拉新消息；
     * 2. 根据当前是否还有分片，决定是否保留 pull topic 的订阅；
     * 3. 最后把 newly-acquired 分片重新投递到本地阻塞队列，由新的 owner 去抢锁并开始拉取。
     * 这个顺序保证：
     * 1. 旧 owner 会渐进排空，不会粗暴释放锁；
     * 2. 新 owner 即使马上开始入队，也只能在锁可用时接手；
     * 3. 多节点扩缩容时，负载迁移过程更平滑。
     */
    private void applyAssignments(Map<String, List<String>> previousAssignments, Map<String, List<String>> currentAssignments) {
        Set<String> allQueues = new LinkedHashSet<>();
        allQueues.addAll(previousAssignments.keySet());
        allQueues.addAll(currentAssignments.keySet());

        for (String queueName : allQueues) {
            RedisMQListenerContainer container = redisListenerContainerManager.getRedisistenerContainer(queueName);
            if (container == null) {
                continue;
            }
            List<String> previous = previousAssignments.getOrDefault(queueName, new ArrayList<>());
            List<String> current = currentAssignments.getOrDefault(queueName, new ArrayList<>());
            Set<String> released = new LinkedHashSet<>(previous);
            released.removeAll(current);
            // 只对本轮真正释放掉的虚拟队列做 quiesce，保留分片和新获取分片都不在这里处理。
            released.forEach(container::quiesceVirtualQueue);
        }

        boolean isEmpty = currentAssignments.values().stream().allMatch(CollectionUtils::isEmpty);
        if (isEmpty) {
            unSubscribe();
            return;
        }
        subscribe();

        long now = System.currentTimeMillis();
        for (Map.Entry<String, List<String>> entry : currentAssignments.entrySet()) {
            String queueName = entry.getKey();
            Queue queue = QueueManager.getQueue(queueName);
            if (queue == null) {
                log.error("repush queue is null");
                continue;
            }
            Set<String> acquired = new LinkedHashSet<>(entry.getValue());
            acquired.removeAll(previousAssignments.getOrDefault(queueName, new ArrayList<>()));
            // 新获得的分片重新投到本地阻塞队列即可，不在这里直接调用 container.start。
            // 这样仍然复用原有 boss 线程 + 虚拟队列锁机制，入口保持统一。
            acquired.forEach(vq -> enqueueVirtualQueue(queue, vq, now));
        }
    }

    private void enqueueVirtualQueue(Queue queue, String virtualQueue, long timestamp) {
        PushMessage pushMessage = new PushMessage();
        pushMessage.setQueue(virtualQueue);
        pushMessage.setTimestamp(timestamp);
        LinkedBlockingQueue<PushMessage> delayBlockingQueue = redisListenerContainerManager.getDelayBlockingQueue();
        LinkedBlockingQueue<String> linkedBlockingQueue = redisListenerContainerManager.getLinkedBlockingQueue();
        if (queue.isDelayState()) {
            delayBlockingQueue.offer(pushMessage);
        } else {
            linkedBlockingQueue.offer(virtualQueue);
        }
    }
}
