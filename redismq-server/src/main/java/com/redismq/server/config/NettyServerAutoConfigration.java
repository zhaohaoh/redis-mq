package com.redismq.server.config;

import com.redismq.server.store.JdbcStoreStrategy;
import com.redismq.server.store.MessageStoreStrategy;
import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.config.NettyConfig;
import com.redismq.common.connection.RedisMQServerUtil;
import com.redismq.rpc.handler.ServerHandler;
import com.redismq.rpc.proccess.RemoteMessageProcessor;
import com.redismq.rpc.proccess.RemoteServerProccessManager;
import com.redismq.rpc.server.NettyServerBootstrap;
import io.netty.channel.ChannelHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * netty服务器自动配置
 *
 * @author hzh
 * @date 2024/04/30
 */
@Slf4j
@Configuration
@ConditionalOnProperty(value = "spring.redismq.nettyConfig.server.enable",havingValue = "true")
public class NettyServerAutoConfigration {
  ;
    
    /**
     * netty服务器引导程序
     *
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    public NettyServerBootstrap nettyServerBootstrap(List<ChannelHandler> channelHandlers) {
        NettyConfig nettyConfig = GlobalConfigCache.NETTY_CONFIG;
        return new NettyServerBootstrap(channelHandlers, nettyConfig);
    }
    
    @Bean
    public RedisMqServer redisMqServer(RedisMQServerUtil redisMQServerUtil,MessageStoreStrategy messageStoreStrategy){
         log.info("redisMqServer init");
        return new RedisMqServer(redisMQServerUtil,messageStoreStrategy);
    }
    
    /**
     * 远程服务器接收任务消息管理器
     *
     * @return {@link RemoteServerProccessManager}
     */
    @Bean
    public RemoteServerProccessManager remoteServerProccessManager(List<RemoteMessageProcessor> mqMessageProcessors) {
        RemoteServerProccessManager remoteServerProccessManager = new RemoteServerProccessManager();
        mqMessageProcessors.forEach(remoteServerProccessManager::registerProcessor);
        return remoteServerProccessManager;
    }
    
    /**
     * 服务器处理程序
     *
     */
    @Bean
    public ChannelHandler serverHandler(RemoteServerProccessManager remoteServerProccessManager) {
        return new ServerHandler(remoteServerProccessManager);
    }
    
    @Bean
    @ConditionalOnProperty(value = "spring.redismq.storeConfig.storeType",havingValue = "mysql",matchIfMissing = true)
    public MessageStoreStrategy messageStoreStrategy(JdbcTemplate jdbcTemplate){
        return  new JdbcStoreStrategy(jdbcTemplate);
    }
}
