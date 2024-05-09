package com.redismq.rpc.client;

import com.redismq.common.config.NettyConfig;
import com.redismq.common.constant.TransportServerType;
import com.redismq.rpc.codec.ProtocolV1Decoder;
import com.redismq.rpc.codec.ProtocolV1Encoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NettyClientBootstrap {
    
    private final NettyConfig nettyConfig;
    
    private final Bootstrap bootstrap = new Bootstrap();
    
    private final EventLoopGroup eventLoopGroupWorker;
    
    private final List<ChannelHandler> channelHandlers;
    
    public NettyClientBootstrap(List<ChannelHandler> channelHandlers,NettyConfig nettyConfig) {
        this.nettyConfig = nettyConfig;
        NettyConfig.Client client = nettyConfig.getClient();
        this.eventLoopGroupWorker = new NioEventLoopGroup(client.getClientThreadSize());
        this.channelHandlers=channelHandlers;
    }
    
    public void start() {
        NettyConfig.Client client = nettyConfig.getClient();
        this.bootstrap.group(this.eventLoopGroupWorker).channel(nettyConfig.getClientChannelClazz())
                .option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, client.getConnectTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF, client.getClientSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, client.getClientSocketRcvBufSize());
        
        if (nettyConfig.getTransportServerType().equals(TransportServerType.NATIVE)) {
            if (PlatformDependent.isOsx()) {
                if (log.isInfoEnabled()) {
                    log.info("client run on macOS");
                }
            } else {
                //linux
                bootstrap.option(EpollChannelOption.EPOLL_MODE, EpollMode.EDGE_TRIGGERED)
                        .option(EpollChannelOption.TCP_QUICKACK, true);
            }
        }
        
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ProtocolV1Encoder());
                pipeline.addLast(new ProtocolV1Decoder());
                pipeline.addLast(new IdleStateHandler(nettyConfig.getHeartbeatReadSeconds(),
                        nettyConfig.getHeartbeatWriteSeconds(),
                        0));
                if (channelHandlers != null) {
                    channelHandlers.forEach(pipeline::addLast);
                }
            }
        });
    }
    
    public void shutdown() {
        try {
            this.eventLoopGroupWorker.shutdownGracefully();
        } catch (Exception exx) {
            log.error("Failed to shutdown: {}", exx.getMessage());
        }
    }
    
    /**
     * Gets new channel.
     *
     * @param address the address
     * @return the new channel
     */
    public Channel getNewChannel(InetSocketAddress address) {
        Channel channel;
        ChannelFuture f = this.bootstrap.connect(address);
        try {
            NettyConfig.Client client = nettyConfig.getClient();
            f.await(client.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (f.isCancelled()) {
                throw new RuntimeException( "connect cancelled, can not connect to services-server.",f.cause());
            } else if (!f.isSuccess()) {
                throw new RuntimeException(  "connect failed, can not connect to services-server.",f.cause());
            } else {
                channel = f.channel();
            }
        } catch (Exception e) {
            throw new RuntimeException( "can not connect to redismq-server.",e);
        }
        return channel;
    }
    
}
