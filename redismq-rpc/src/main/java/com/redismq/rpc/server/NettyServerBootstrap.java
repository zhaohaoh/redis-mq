package com.redismq.rpc.server;

import com.redismq.common.config.NettyConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.netty.channel.ChannelOption.SO_REUSEADDR;

/**
 * @author hzh
 * @date 2020/11/14 10:48
 * netty采用nio处理模型，以一个boss，丢给多个work。不停轮询多个客户端请求。
 * netty自带了一个EventExecutorGroup的线程池封装。这种线程池和NioEventLoop和channel一一对应。一个channel对应一个线程。
 * 简单说就是EventExecutorGroup处理多客户端并发。因为多个客户端对应多个EventExecutorGroup。每个客户端又有多个请求
 * 自定义线程池处理比较少的客户端请求。他会对所有请求都进行线程处理。并且IO密集型的操作创建更多线程才有用
 */
@Slf4j
public class NettyServerBootstrap {
    
    private final NettyConfig nettyConfig;
    
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    //boss线程是分发器，将任务丢给work
    private final EventLoopGroup boss = new NioEventLoopGroup(1);
    //work默认为cpu线程数。
    private final EventLoopGroup work = new NioEventLoopGroup();

//    @Resource
//    private HeartbeatHandlerAdapter heartbeatHandler;
    private final List<ChannelHandler> channelHandlers;
    
    public NettyServerBootstrap(List<ChannelHandler> channelHandlers,NettyConfig  nettyConfig) {
        this.channelHandlers = channelHandlers;
        this.nettyConfig = nettyConfig;
    }
    
   
    public void start() {
        
        Class<? extends ServerChannel> serverChannelClazz = nettyConfig.getServerChannelClazz();
        NettyConfig.Server server = nettyConfig.getServer();
        serverBootstrap.group(boss, work)
                //使用nio
                .channel(serverChannelClazz)
                //链接队列
                .option(ChannelOption.SO_BACKLOG, server.getSoBackLogSize())
                .option(SO_REUSEADDR,true)
                //心跳检测，2小时
                .option(ChannelOption.SO_KEEPALIVE, true)
                //true是禁止nagle算法进行延迟批量发送 禁止延迟发送
                .childOption(ChannelOption.TCP_NODELAY,true)
                // 发送缓冲区大小
                .childOption(ChannelOption.SO_SNDBUF, server.getServerSocketSendBufSize())
                // 接收缓存区大小
                .childOption(ChannelOption.SO_RCVBUF, server.getServerSocketResvBufSize())
                //写缓存区的高低水位线
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(server.getWriteBufferLowWaterMark(),
                                server.getWriteBufferHighWaterMark()))
                .handler(new LoggingHandler(LogLevel.INFO));
        try {
            //child是处理workerGroup，handler是处理boss
            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    //通道连接中的管道
                    ChannelPipeline pipeline = ch.pipeline();
                    //心跳检测，3分钟读取不到数据关闭客户端连接,自动下线
                    pipeline.addLast(new IdleStateHandler(nettyConfig.getHeartbeatReadSeconds(), nettyConfig.getHeartbeatWriteSeconds()
                            , 0, TimeUnit.SECONDS));
                    //对心跳检测进一步处理的Handler  观察者模式
//                    pipeline.addLast(heartbeatHandler);
                    //StringDecoder将接受的码流转换为字符串
                    pipeline.addLast(new StringDecoder(StandardCharsets.UTF_8));
                    pipeline.addLast(new StringEncoder(StandardCharsets.UTF_8));
                    //LineBasedFrameDecoder遍历ByteBuf中的可读字节，按行（\n \r\n）处理
                    pipeline.addLast(new LineBasedFrameDecoder(65535));
//                    pipeline.addLast(channelHandlerAdapter);
//                    pipeline.addLast(channelOutHandlerAdapter);
                    channelHandlers.forEach(pipeline::addLast);
                }
            });
            int port = server.getPort();
            log.info("NettyServer  start bind port:{}", port);
            //绑定服务器
            ChannelFuture f = serverBootstrap.bind(port).sync();
            //监听服务器通道关闭
//            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("NettyServer  InterruptedException stop");
            //关闭boss和work服务线程
            boss.shutdownGracefully();
            work.shutdownGracefully();
        }
    }
    
    public void close() {
        shutdown();
    }
    
    public void shutdown() {
        log.info("[netty服务]关闭服务器....");
        boss.shutdownGracefully();
        work.shutdownGracefully();
    }

}