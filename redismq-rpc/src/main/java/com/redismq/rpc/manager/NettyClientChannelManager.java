package com.redismq.rpc.manager;

import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.Server;
import com.redismq.common.util.NetUtil;
import com.redismq.common.util.ServerManager;
import com.redismq.rpc.util.ServerUtil;
import io.netty.channel.Channel;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *如果你需要标记一个对象为无效，你可以从对象池中借出该对象，
 * 调用其业务逻辑中的某个方法（如果适用）来标记其无效，
 * 然后调用 invalidateObject 方法。
 * 但请注意，invalidateObject 并不直接删除对象，而是将其标记为不再可用。
 * Pool2 会在后续操作中（如 returnObject）检查对象的有效性，并根据需要采取相应措施。
 */
public class NettyClientChannelManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientChannelManager.class);
    
    private final ConcurrentMap<String, Object> channelLocks = new ConcurrentHashMap<>();
    
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    
    private final GenericKeyedObjectPool<AddressInfo, Channel> nettyClientKeyPool;
    
    private final ConcurrentMap<String, AddressInfo> addressKeyMap =new ConcurrentHashMap<>();
    public NettyClientChannelManager(final NettyPoolableFactory keyPoolableFactory) {
        nettyClientKeyPool = new GenericKeyedObjectPool<>(keyPoolableFactory);
        nettyClientKeyPool.setConfig(getNettyPoolConfig());
    }
    private GenericKeyedObjectPoolConfig  getNettyPoolConfig() {
        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setMaxTotal(8);
        // 最多的空闲连接数
        poolConfig.setMaxIdlePerKey(8);
        poolConfig.setMinIdlePerKey(0);
        poolConfig.setMaxWait(Duration.ofSeconds(60));
        //校验有效性
        poolConfig.setTestOnBorrow(true);
        //归还时校验有效性
        poolConfig.setTestOnCreate(true);
        poolConfig.setLifo(true);
        return poolConfig;
    }
    
    
    /**
     * Get all channels registered on current Rpc Client.
     *
     * @return channels
     */
    public ConcurrentMap<String, Channel> getChannels() {
        return channels;
    }
    
    /**
     * Acquire netty client channel connected to remote server.
     */
    public Channel acquireChannel(String serverAddress) {
       
        Channel channelToServer = channels.get(serverAddress);
        if (channelToServer != null) {
            //如果服务已经下线了
            if (!channelToServer.isActive()) {
                if (!serverActive(serverAddress)) {
                    LOGGER.info("server is down  so return acquireChannel");
                    releaseChannel(serverAddress, channelToServer);
                    return null;
                }
            }
            channelToServer = getExistAliveChannel(channelToServer, serverAddress);
            if (channelToServer != null) {
                return channelToServer;
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will connect to {}", serverAddress);
        }
        return doConnect(serverAddress);
    }
    
    private boolean serverActive(String serverAddress) {
        Set<Server> remoteAvailServerList = ServerManager.getRemoteAvailServers();
        Optional<Server> first = remoteAvailServerList.stream()
                .filter(server -> server.getAddress().equals(serverAddress)).findFirst();
        return first.isPresent();
    }
    
    /**
     * Release channel to pool if necessary.
     *
     * @param channel       channel
     * @param serverAddress server address
     */
    public void releaseChannel(String serverAddress,Channel channel) {
        if (channel == null || serverAddress == null) {
            return;
        }
        try {
            synchronized (channelLocks.get(serverAddress)) {
                Channel ch = channels.get(serverAddress);
                AddressInfo addressInfo = getAddressInfo(serverAddress);
                if (ch == null) {
                    nettyClientKeyPool.returnObject(addressInfo, channel);
                    return;
                }
                if (ch.compareTo(channel) == 0) {
//                    if (LOGGER.isInfoEnabled()) {
//                        LOGGER.info("return to pool, rm channel:{}", channel);
//                    }
                    destroyChannel(serverAddress, channel);
                } else {
                    nettyClientKeyPool.returnObject(addressInfo, channel);
                }
            }
        } catch (Exception exx) {
            LOGGER.error(exx.getMessage());
        }
    }
    
    /**
     * Destroy channel.
     *
     * @param serverAddress server address
     * @param channel       channel
     */
    public void destroyChannel(String serverAddress, Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            if (channel.equals(channels.get(serverAddress))) {
                channels.remove(serverAddress);
            }
            AddressInfo addressInfo = getAddressInfo(serverAddress);
            nettyClientKeyPool.returnObject(addressInfo, channel);
        } catch (Exception exx) {
            LOGGER.error("return channel to rmPool error:{}", exx.getMessage());
        }
    }
    public void destroyChannel(Channel channel) {
      destroyChannel(NetUtil.getAddressFromChannel(channel),channel);
    }
    
    /**
     *
     */
    public void reconnect() {
        Set<Server> availList = ServerUtil.getAvailServerList();
        if (CollectionUtils.isEmpty(availList)) {
            return;
        }
        //根据ip和端口获取有效的channel
        for (Server serverAddress : availList) {
            acquireChannel(serverAddress.getAddress());
        }
    }
    
    public void invalidateObject(final String serverAddress, final Channel channel) {
        AddressInfo addressInfo = getAddressInfo(serverAddress);
        try {
            nettyClientKeyPool.invalidateObject(addressInfo, channel);
            LOGGER.info("invalidateObject address:{} channel:{}",addressInfo,channel);
        } catch (Exception e) {
            LOGGER.error("invalidateObject error", e);
        }
    }
    
    private AddressInfo getAddressInfo(String serverAddress) {
        return addressKeyMap.computeIfAbsent(serverAddress, k -> {
            AddressInfo address = new AddressInfo();
            address.setTargetAddress(serverAddress);
            return address;
        });
    }
    
    public void registerChannel(final String serverAddress, final Channel channel) {
        Channel channelToServer = channels.get(serverAddress);
        if (channelToServer != null && channelToServer.isActive()) {
            return;
        }
        channels.put(serverAddress, channel);
    }
    
    private Channel doConnect(String serverAddress) {
        Object lockObj = channelLocks.computeIfAbsent(serverAddress, key -> new Object());
        Channel channelFromPool;
        synchronized (lockObj) {
            Channel channelToServer = channels.get(serverAddress);
            if (channelToServer != null && channelToServer.isActive()) {
                return channelToServer;
            }
            
            try {
                AddressInfo addressInfo = getAddressInfo(serverAddress);
                channelFromPool = nettyClientKeyPool.borrowObject(addressInfo);
                registerChannel(serverAddress, channelFromPool);
            } catch (Exception exx) {
                LOGGER.error("doConnect  failed", exx);
                Server server = new Server();
                server.setAddress(serverAddress);
                ServerManager.removeExpireServer(server);
                return null;
            }
        }
        return channelFromPool;
    }
    
    
    
    private Channel getExistAliveChannel(Channel channel, String serverAddress) {
        if (channel.isActive()) {
            return channel;
        } else {
            int i = 0;
            for (; i < 100; i++) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
                channel = channels.get(serverAddress);
                if (channel != null && channel.isActive()) {
                    return channel;
                }
            }
            if (i == 100) {
                LOGGER.warn("channel {} is not active after long wait, close it.", channel);
                releaseChannel( serverAddress,channel);
                return null;
            }
        }
        return null;
    }
}

