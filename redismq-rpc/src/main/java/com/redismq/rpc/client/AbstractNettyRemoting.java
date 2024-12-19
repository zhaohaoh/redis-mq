package com.redismq.rpc.client;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.MessageType;
import com.redismq.common.exception.RedisMQRpcException;
import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.MergedRemoteMessage;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteMessageFuture;
import com.redismq.common.pojo.Server;
import com.redismq.common.rebalance.RandomBalance;
import com.redismq.common.rebalance.ServerSelectBalance;
import com.redismq.common.util.NetUtil;
import com.redismq.common.util.RpcMessageUtil;
import com.redismq.common.util.ServerManager;
import com.redismq.rpc.manager.NettyClientChannelManager;
import com.redismq.rpc.util.ServerUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.redismq.rpc.cache.RpcGlobalCache.REMOTE_FUTURES;

@Slf4j
public class AbstractNettyRemoting implements RemotingClient {
    
    public static final Object LOCK = new Object();
    
    protected final Object mergeLock = new Object();
    
    protected final int MAX_MERGE_SEND_MILLS = GlobalConfigCache.NETTY_CONFIG.getMaxMergeSendMills();
    
    protected boolean isSending = false;
    
    protected final ConcurrentHashMap<String/*serverAddress*/, BlockingQueue<RemoteMessage>> basketMap = new ConcurrentHashMap<>();
    
    protected NettyClientChannelManager nettyClientChannelManager;
    
    protected final ServerSelectBalance selectBalance = new RandomBalance();
    
    private final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1);
    
    private ExecutorService mergeSendExecutorService;
    
    
    public AbstractNettyRemoting(NettyClientChannelManager nettyClientChannelManager) {
        this.nettyClientChannelManager = nettyClientChannelManager;
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
            for (Map.Entry<String, RemoteMessageFuture> entry : REMOTE_FUTURES.entrySet()) {
                RemoteMessageFuture future = entry.getValue();
                if (future.isTimeout()) {
                    REMOTE_FUTURES.remove(entry.getKey());
                    RemoteMessage rpcMessage = future.getRequestMessage();
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
     * 发送同步消息
     */
    @Override
    public Object sendSync(Object msg, int messageType) {
        if (!GlobalConfigCache.NETTY_CONFIG.getClient().isHealth() && CollectionUtils
                .isEmpty(ServerManager.getLocalAvailServers())) {
            log.warn("redismq-server not found  no sendSync msg:{}",msg);
            return null;
        }
        
        String serverAddress = ServerUtil.loadBalance(null, msg);
        
        long timeoutMillis = GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout();
        
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        
        RemoteMessage remoteMessage = RpcMessageUtil.buildRequestMessage(msg, addressInfo, messageType);
        RemoteMessageFuture messageFuture = new RemoteMessageFuture();
        messageFuture.setRequestMessage(remoteMessage);
        messageFuture.setTimeout(timeoutMillis);
        REMOTE_FUTURES.put(remoteMessage.getId(), messageFuture);
        
        // put message into basketMap
        BlockingQueue<RemoteMessage> basket = basketMap
                .computeIfAbsent(serverAddress, key -> new LinkedBlockingQueue<>());
        if (!basket.offer(remoteMessage)) {
            log.error("put message into basketMap offer failed, serverAddress:{},remoteMessage:{}", serverAddress,
                    remoteMessage);
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug("offer message: {}", remoteMessage.getBody());
        }
        //唤醒异步合并发送线程
        if (!isSending) {
            synchronized (mergeLock) {
                mergeLock.notifyAll();
            }
        }
        
        //同步阻塞等待响应结果
        try {
            return messageFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception exx) {
            log.error("wait response error:{},ip:{},request:{}", exx.getMessage(), serverAddress,
                    remoteMessage.getBody());
            if (exx instanceof TimeoutException) {
                throw new RedisMQRpcException("rcp timeout ", exx);
            } else {
                throw new RedisMQRpcException("rcp error ", exx);
            }
        }
    }
    
    
    @Override
    public void sendAsync(Object msg, int messageType) {
        if (!GlobalConfigCache.NETTY_CONFIG.getClient().isHealth() && CollectionUtils
                .isEmpty(ServerManager.getLocalAvailServers())) {
            return;
        }
        
        String serverAddress = ServerUtil.loadBalance(null, msg);
        long timeoutMillis = GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout();
        
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        
        RemoteMessage remoteMessage = RpcMessageUtil.buildRequestMessage(msg, addressInfo, messageType);
        //异步发送暂时不用给结果
        //        RemoteMessageFuture messageFuture = new RemoteMessageFuture();
        //        messageFuture.setRequestMessage(remoteMessage);
        //        messageFuture.setTimeout(timeoutMillis);
        //        REMOTE_FUTURES.put(remoteMessage.getId(), messageFuture);
        
        // put message into basketMap
        BlockingQueue<RemoteMessage> basket = basketMap
                .computeIfAbsent(serverAddress, key -> new LinkedBlockingQueue<>());
        //如果推入异步队列失败的话直接异步发送
        if (!basket.offer(remoteMessage)) {
            log.error("put message into basketMap offer failed, serverAddress:{},remoteMessage:{}", serverAddress,
                    remoteMessage);
            doSendAsync(serverAddress, remoteMessage);
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("offer message: {}", remoteMessage.getBody());
        }
        //唤醒异步合并发送线程
        if (!isSending) {
            synchronized (mergeLock) {
                mergeLock.notifyAll();
            }
        }
    }
    
    
    private ChannelFuture doSendAsync(String serverAddress, RemoteMessage remoteMessage) {
        Channel channel = nettyClientChannelManager.acquireChannel(serverAddress);
        //提前校验消息是否可写 否则可能会导致内存溢出或者消息丢失
        channelWritableCheck(channel, remoteMessage);
        // 异步消费完成才算消费成功
        return channel.writeAndFlush(remoteMessage);
    }
    
    /**
     * 发送异步消息
     */
    private void sendBatchAsync(String serverAddress, MergedRemoteMessage mergedWarpMessage) {
        if (!GlobalConfigCache.NETTY_CONFIG.getClient().isHealth() && CollectionUtils
                .isEmpty(ServerManager.getLocalAvailServers())) {
            return;
        }
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        RemoteMessage remoteMessage = RpcMessageUtil
                .buildRequestMessage(mergedWarpMessage, addressInfo, MessageType.BATCH_MESSAGE);
        Channel channel = nettyClientChannelManager.acquireChannel(serverAddress);
        //提前校验消息是否可写 否则可能会导致内存溢出或者消息丢失
        channelWritableCheck(channel, remoteMessage);
        channel.writeAndFlush(remoteMessage).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.error("rpc writeAndFlush message err ", future.cause());
                RemoteMessageFuture remove = REMOTE_FUTURES.remove(remoteMessage.getId());
                if (remove != null) {
                    remove.setResultMessage(new RedisMQRpcException("rpc writeAndFlush message err ", future.cause()));
                }
                nettyClientChannelManager.destroyChannel(future.channel());
            }
        });
    }
    
    /**
     * 发送同步消息
     */
    @Override
    public Object sendBatchSync(List<?> msg, int messageType) {
        if (!GlobalConfigCache.NETTY_CONFIG.getClient().isHealth() && CollectionUtils
                .isEmpty(ServerManager.getLocalAvailServers())) {
            return null;
        }
        String serverAddress = ServerUtil.loadBalance(null, msg);
        long timeoutMillis = GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout();
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        
        List<RemoteMessage> remoteMessages = msg.stream()
                .map(o -> RpcMessageUtil.buildRequestMessage(o, addressInfo, messageType)).collect(Collectors.toList());
        
        BlockingQueue<RemoteMessage> basket = basketMap
                .computeIfAbsent(serverAddress, key -> new LinkedBlockingQueue<>());
        
        List<RemoteMessageFuture> remoteMessageFutures = new ArrayList<>();
        for (RemoteMessage message : remoteMessages) {
            RemoteMessageFuture future = new RemoteMessageFuture();
            future.setRequestMessage(message);
            future.setTimeout(timeoutMillis);
            remoteMessageFutures.add(future);
            
            REMOTE_FUTURES.put(message.getId(), future);
            if (!basket.offer(message)) {
                log.error("put message into basketMap offer failed, serverAddress:{},remoteMessage:{}", serverAddress,
                        message);
            }
        }
        
        //唤醒异步合并发送线程
        if (!isSending) {
            synchronized (mergeLock) {
                mergeLock.notifyAll();
            }
        }
        
        //同步阻塞等待响应结果
        try {
            for (RemoteMessageFuture remoteMessageFuture : remoteMessageFutures) {
                Object result = remoteMessageFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
                if (result != null && !"true".equals(String.valueOf(result))) {
                    log.error("sendBatchSync rpc result error :{}", result);
                    throw new RedisMQRpcException("sendBatchSync rpc result error");
                }
            }
            return true;
        } catch (Exception exx) {
            log.error("wait response error:{},ip:{},request:{}", exx.getMessage(), serverAddress,
                    remoteMessages.toArray());
            if (exx instanceof TimeoutException) {
                throw new RedisMQRpcException("rpc timeout ", exx);
            } else {
                throw new RedisMQRpcException("rpc error ", exx);
            }
        }
    }
    
    
    protected void channelWritableCheck(Channel channel, Object msg) {
        int tryTimes = 0;
        synchronized (LOCK) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    if (tryTimes > 2000) {
                        nettyClientChannelManager.destroyChannel(NetUtil.getAddressFromChannel(channel), channel);
                        throw new RuntimeException("channelWritableCheck  channel is Not Writable" + msg.toString());
                    }
                    LOCK.wait(10);
                } catch (InterruptedException exx) {
                    log.error(exx.getMessage());
                }
            }
        }
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
                basketMap.forEach((address, basket) -> {
                    if (basket.isEmpty()) {
                        return;
                    }
                    
                    MergedRemoteMessage mergeMessage = new MergedRemoteMessage();
                    try {
                        while (!basket.isEmpty()) {
                            RemoteMessage msg = basket.poll();
                            List<RemoteMessage> messages = mergeMessage.getMessages();
                            messages.add(msg);
                        }
                        
                        sendBatchAsync(address, mergeMessage);
                    } catch (Exception e) {
                        log.error("client merge message rpc call failed: {}", e.getMessage(), e);
                        List<RemoteMessage> messages = mergeMessage.getMessages();
                        for (RemoteMessage message : messages) {
                            String id = message.getId();
                            RemoteMessageFuture messageFuture = REMOTE_FUTURES.remove(id);
                            if (messageFuture != null) {
                                messageFuture.setResultMessage(
                                        new RuntimeException(String.format("%s is unreachable", address), e));
                            }
                        }
                    }
                });
                isSending = false;
            }
        }
    }
}
