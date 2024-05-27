package com.enable;

import com.redismq.autoconfigure.RedisMQAutoConfiguration;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.exception.RedisMqException;
import com.redismq.common.pojo.Server;
import com.redismq.rpc.client.NettyClientBootstrap;
import com.redismq.rpc.client.NettyRemotingClient;
import com.redismq.rpc.client.RemotingClient;
import com.redismq.rpc.handler.ClientHandler;
import com.redismq.rpc.manager.NettyClientChannelManager;
import com.redismq.rpc.manager.NettyPoolableFactory;
import com.redismq.rpc.util.ServerUtil;
import io.netty.channel.ChannelHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

@Slf4j
@Configuration
@ConditionalOnProperty(value = "spring.redismq.netty-config.client.enable", havingValue = "true")
@AutoConfigureAfter(RedisMQAutoConfiguration.class)
public class NettyClientAutoConfigration implements SmartInitializingSingleton {
    
    /**
     * netty客户端引导程序
     */
    @Bean(initMethod = "start")
    public NettyClientBootstrap nettyClientBootstrap(List<ChannelHandler> channelHandlers) {
        NettyConfig nettyConfig = GlobalConfigCache.NETTY_CONFIG;
        return new NettyClientBootstrap(channelHandlers, nettyConfig);
    }
    
    /**
     * netty通道管理器
     */
    @Bean
    public NettyClientChannelManager nettyClientChannelManager(@Lazy NettyClientBootstrap nettyClientBootstrap) {
        NettyPoolableFactory nettyPoolableFactory = new NettyPoolableFactory(nettyClientBootstrap);
        NettyClientChannelManager nettyClientChannelManager = new NettyClientChannelManager(nettyPoolableFactory);
        return nettyClientChannelManager;
    }
    
    /**
     * 客户端处理程序
     */
    @Bean
    public ChannelHandler clientHandler(NettyClientChannelManager nettyClientChannelManager) {
        return new ClientHandler(nettyClientChannelManager);
    }
    
    /**
     * RPC通信客户端
     */
    @Bean
    public RemotingClient remotingClient(NettyClientChannelManager nettyClientChannelManager) {
        return new NettyRemotingClient(nettyClientChannelManager);
    }
    
    @SneakyThrows
    @Override
    public void afterSingletonsInstantiated() {
        if (!GlobalConfigCache.NETTY_CONFIG.getClient().isEnable()) {
            return;
        }
        Set<Server> availServerList = null;
        long currentTimeMillis = System.currentTimeMillis();
        int timeout = GlobalConfigCache.NETTY_CONFIG.getClient().getSelectServerTimeoutMillis();
        boolean health = GlobalConfigCache.NETTY_CONFIG.getClient().isHealth();
        if (health) {
            while (CollectionUtils.isEmpty(availServerList)) {
                availServerList = ServerUtil.getAvailServerList();
                if (CollectionUtils.isEmpty(availServerList)) {
                    if (System.currentTimeMillis() - currentTimeMillis > timeout) {
                        throw new RedisMqException("load balance server error");
                    }
                    log.error("load balance server error,waiting...");
                    Thread.sleep(500L);
                }
            }
        }
    }
}
