package com.redismq.server.config;


import com.redismq.server.store.MessageStoreStrategy;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.connection.RedisMQServerUtil;
import com.redismq.common.pojo.Server;
import com.redismq.common.pojo.ServerRegisterInfo;
import com.redismq.common.util.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @Author: hzh
 */
public class RedisMqServer {
    
    protected static final Logger log = LoggerFactory.getLogger(RedisMqServer.class);
    
    /**
     * 注册线程客户端维持心跳线程
     */
    private final ScheduledThreadPoolExecutor registerThread = new ScheduledThreadPoolExecutor(1);
    
    /**
     * 清理过期消息
     */
    private final ScheduledThreadPoolExecutor clearExpireMessage = new ScheduledThreadPoolExecutor(1);
    
    /**
     * RedisMQServerUtil
     */
    private final RedisMQServerUtil redisMQServerUtil;
    
    private final MessageStoreStrategy messageStoreStrategy;
    
    
    public RedisMqServer(RedisMQServerUtil redisMQServerUtil, MessageStoreStrategy messageStoreStrategy) {
        this.redisMQServerUtil = redisMQServerUtil;
        this.messageStoreStrategy = messageStoreStrategy;
    }
    
    /**
     * 注册服务
     *
     */
    public void registerServer() {
        try {
            String address = getAddress();
            Server server = new Server();
            server.setAddress(address);
            redisMQServerUtil.registerServer(server);
            ServerRegisterInfo serverRegisterInfo = new ServerRegisterInfo();
            serverRegisterInfo.setRegister(true);
            serverRegisterInfo.setAddress(server.getAddress());
            redisMQServerUtil.publishServer(serverRegisterInfo);
        } catch (Exception e) {
            log.error("registerServer error : ", e);
        }
        
    }
    
    /**
     * 开始注册服务端任务
     */
    @PostConstruct
    public void startRegisterServerTask() {
        NettyConfig nettyConfig = GlobalConfigCache.NETTY_CONFIG;
        int serverRegisterExpireSeconds = nettyConfig.getServer().getServerRegisterExpireSeconds();
        registerThread
                .scheduleAtFixedRate(this::registerServer, serverRegisterExpireSeconds, serverRegisterExpireSeconds,
                        TimeUnit.SECONDS);
        
        clearExpireMessage.scheduleAtFixedRate(this::clearExpireMessage, 0, 1, TimeUnit.DAYS);
        log.info("redisMqServer  startRegisterServerTask");
    }
    
    /**
     * 清除过期消息
     */
    public void clearExpireMessage() {
        messageStoreStrategy.clearExpireMessage();
    }
    
    /**
     * 删除下线服务
     */
    @PreDestroy
    public void removeServer() {
        String address = getAddress();
        Server server = new Server();
        server.setAddress(address);
        redisMQServerUtil.removeServer(server);
        ServerRegisterInfo serverRegisterInfo = new ServerRegisterInfo();
        serverRegisterInfo.setRegister(false);
        serverRegisterInfo.setAddress(server.getAddress());
        redisMQServerUtil.publishServer(serverRegisterInfo);
        log.error("removeServer :{}", serverRegisterInfo);
    }
    
    private static String getAddress() {
        String host = GlobalConfigCache.NETTY_CONFIG.getServer().getHost();
        if (StringUtils.isBlank(host)){
            host = NetUtil.getLocalIp();
        }
        int port = GlobalConfigCache.NETTY_CONFIG.getServer().getPort();
        String address = (host + ":" + port);
        return address;
    }
    
}
