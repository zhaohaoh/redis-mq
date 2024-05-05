package com.enable;

import com.redismq.autoconfigure.RedisMQAutoConfiguration;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.rpc.client.NettyClientBootstrap;
import com.redismq.rpc.client.NettyRemotingClient;
import com.redismq.rpc.client.RemotingClient;
import com.redismq.rpc.handler.ClientHandler;
import com.redismq.rpc.manager.NettyClientChannelManager;
import com.redismq.rpc.manager.NettyPoolableFactory;
import io.netty.channel.ChannelHandler;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.List;

@Configuration
@ConditionalOnProperty(value = "spring.redismq.netty-config.client.enable",havingValue = "true")
@AutoConfigureAfter(RedisMQAutoConfiguration.class)
public class NettyClientAutoConfigration {

    /**
     * netty客户端引导程序
     *
     */
    @Bean(initMethod = "start")
    public NettyClientBootstrap nettyClientBootstrap(List<ChannelHandler> channelHandlers) {
        NettyConfig nettyConfig = GlobalConfigCache.NETTY_CONFIG;
        return new NettyClientBootstrap(channelHandlers, nettyConfig);
    }
    /**
     * netty通道管理器
     *
     */
    @Bean
    public NettyClientChannelManager nettyClientChannelManager(@Lazy NettyClientBootstrap nettyClientBootstrap){
        NettyPoolableFactory nettyPoolableFactory = new NettyPoolableFactory(nettyClientBootstrap);
        NettyClientChannelManager nettyClientChannelManager = new NettyClientChannelManager(nettyPoolableFactory);
        return nettyClientChannelManager;
    }
    
    /**
     * 客户端处理程序
     *
     */
    @Bean
    public ChannelHandler clientHandler(NettyClientChannelManager nettyClientChannelManager) {
        return new ClientHandler(nettyClientChannelManager);
    }
    /**
     *  RPC通信客户端
     */
    @Bean
    public RemotingClient remotingClient(NettyClientChannelManager nettyClientChannelManager){
        return new NettyRemotingClient(nettyClientChannelManager);
    }
}
