package com.redismq.rpc.client;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.MessageType;
import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.MergedWarpMessage;
import com.redismq.common.pojo.MessageFuture;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.Server;
import com.redismq.common.rebalance.RandomBalance;
import com.redismq.common.rebalance.ServerSelectBalance;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.common.util.NetUtil;
import com.redismq.common.util.RpcMessageUtil;
import com.redismq.common.util.ServerManager;
import com.redismq.rpc.manager.NettyClientChannelManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class AbstractNettyRemoting implements RemotingClient {
    
    public static final Object LOCK = new Object();
    
    protected NettyClientChannelManager nettyClientChannelManager;
    
    protected final Object mergeLock = new Object();
    
    protected final int MAX_MERGE_SEND_MILLS = GlobalConfigCache.NETTY_CONFIG.getMaxMergeSendMills();
    
    protected boolean isSending = false;
    
    protected final ConcurrentHashMap<String/*serverAddress*/, BlockingQueue<RemoteMessage>> basketMap = new ConcurrentHashMap<>();
    
    protected final ConcurrentHashMap<String, MessageFuture> futures = new ConcurrentHashMap<>();
    
    protected final ServerSelectBalance selectBalance = new RandomBalance();
    
    protected final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1);
    
    
    public AbstractNettyRemoting(NettyClientChannelManager nettyClientChannelManager) {
        this.nettyClientChannelManager = nettyClientChannelManager;
    }
    
    /**
     * 初始化， 清理超时任务
     */
    public void init() {
        timerExecutor.scheduleAtFixedRate(() -> {
            for (Map.Entry<String, MessageFuture> entry : futures.entrySet()) {
                MessageFuture future = entry.getValue();
                if (future.isTimeout()) {
                    futures.remove(entry.getKey());
                    RemoteMessage rpcMessage = future.getRequestMessage();
                    future.setResultMessage(new TimeoutException(String
                            .format("msgId: %s ,msgType: %s ,msg: %s ,request timeout", rpcMessage.getId(),
                                    rpcMessage.getMessageType(), rpcMessage.getBody().toString())));
                }
            }
        }, 5000, 5000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 发送异步消息
     */
    @Override
    public void sendAsync(Object msg) {
        String serverAddress = loadBalance(null, msg);
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        RemoteMessage remoteMessage = RpcMessageUtil.buildRequestMessage(msg, addressInfo, MessageType.MESSAGE);
        Channel channel = nettyClientChannelManager.acquireChannel(serverAddress);
        //提前校验消息是否可写 否则可能会导致内存溢出或者消息丢失
        channelWritableCheck(channel, remoteMessage);
        channel.writeAndFlush(RedisMQStringMapper.toJsonStr(remoteMessage))
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        Throwable cause = future.cause();
                        log.error("channel writeAndFlush error :", cause);
                        nettyClientChannelManager.destroyChannel(future.channel());
                    }
                });
    }
    
    /**
     * 发送异步消息
     */
    @Override
    public void sendBatchAsync(String serverAddress, MergedWarpMessage mergedWarpMessage) {
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        RemoteMessage remoteMessage = RpcMessageUtil.buildRequestMessage(mergedWarpMessage, addressInfo, MessageType.BATCH_MESSAGE);
        Channel channel = nettyClientChannelManager.acquireChannel(serverAddress);
        //提前校验消息是否可写 否则可能会导致内存溢出或者消息丢失
        channelWritableCheck(channel, remoteMessage);
        channel.writeAndFlush(remoteMessage).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                nettyClientChannelManager.destroyChannel(future.channel());
            }
        });
    }
    
    /**
     * 发送同步消息
     */
    @Override
    public Object sendSync(Object msg) throws TimeoutException {
        String serverAddress = loadBalance(null, msg);
        long timeoutMillis = GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout();
        
        AddressInfo addressInfo = new AddressInfo();
        addressInfo.setSourceAddress(serverAddress);
        
        RemoteMessage remoteMessage = RpcMessageUtil.buildRequestMessage(msg, addressInfo, MessageType.MESSAGE);
        MessageFuture messageFuture = new MessageFuture();
        messageFuture.setRequestMessage(remoteMessage);
        messageFuture.setTimeout(timeoutMillis);
        futures.put(remoteMessage.getId(), messageFuture);
        
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
                throw (TimeoutException) exx;
            } else {
                throw new RuntimeException(exx);
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
    
    /**
     * 两个参数暂时无用，预留
     */
    @SuppressWarnings("unchecked")
    protected String loadBalance(String group, Object msg) {
        String address = null;
        try {
            Set<Server> servers = ServerManager.getLocalAvailServers();
            if (CollectionUtils.isEmpty(servers)) {
                return null;
            }
            if (servers.size() == 1) {
                return servers.iterator().next().getAddress();
            }
            List<String> serverList = servers.stream().map(Server::getAddress).collect(Collectors.toList());
            address = selectBalance.select(serverList, msg.toString());
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return address;
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
                    
                    MergedWarpMessage mergeMessage = new MergedWarpMessage();
                    while (!basket.isEmpty()) {
                        RemoteMessage msg = basket.poll();
                        mergeMessage.getMessages().add(msg);
                    }
                    
                    try {
                        sendBatchAsync(address, mergeMessage);
                    } catch (Exception e) {
                        log.error("client merge call failed: {}", e.getMessage(), e);
                        List<RemoteMessage> messages = mergeMessage.getMessages();
                        for (RemoteMessage message : messages) {
                            String id = message.getId();
                            MessageFuture messageFuture = futures.remove(id);
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
