package com.redismq.rpc.handler;

import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.rpc.client.AbstractNettyRemoting;
import com.redismq.rpc.proccess.RemoteServerProccessManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

//单例多线程共享，但线程不安全
@ChannelHandler.Sharable
@Slf4j
public class ServerHandler extends ChannelDuplexHandler {
    
    /**
     * 远程服务器管理器
     */
    private final RemoteServerProccessManager remoteServerProccessManager;
    
    public ServerHandler(RemoteServerProccessManager remoteServerProccessManager) {
        this.remoteServerProccessManager = remoteServerProccessManager;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
       
        log.error("nettyServer error : " ,cause);
        ctx.close();
    }

    /**
     * 在建立连接时发送消息
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        boolean active = channel.isActive();
//        log.info("nettyServer :" + channel.remoteAddress() + " active");
//        ctx.writeAndFlush("[连接server服务成功]:");
    }
    
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        log.info("nettyServer channelWritabilityChanged :" + ctx.channel().isWritable());
        synchronized (AbstractNettyRemoting.LOCK) {
            if (ctx.channel().isWritable()) {
                AbstractNettyRemoting.LOCK.notifyAll();
            }
        }
        super.channelWritabilityChanged(ctx);
    }
    
    /**
     * 处理新加的消息通道
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
//        channel.writeAndFlush("[" + channel.remoteAddress() + "] 加入消息通道连接");
//        log.info("nettyServer :" + channel.remoteAddress() + " new add");
    }


//    /**
//     * 处理退出消息通道
//     *
//     * @param ctx
//     * @throws Exception
//     */
//    @Override
//    public void handlerRemoved(ChannelHandlerContext ctx) {
//         log.info("nettyServer :" + ctx.channel().remoteAddress() + " removed");
//    }


    /**
     * 退出时发送消息
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) {
            log.info("nettyServer :" + channel.remoteAddress() + " is down ");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        //接受json串
        RemoteMessage remoteMessage = (RemoteMessage) msg;
        log.info("nettyServer Receive message :" + ctx.channel().remoteAddress() + remoteMessage);
        
//        RemoteMessage remoteMessage = RedisMQStringMapper.toBean(data, RemoteMessage.class);
        RemoteResponse remoteResponse = new RemoteResponse();
        remoteResponse.setChannelHandlerContext(ctx);
        remoteServerProccessManager.processMessage(remoteResponse,remoteMessage);
    }


}