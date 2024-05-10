package com.redismq.rpc.handler;


import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteMessageFuture;
import com.redismq.common.util.NetUtil;
import com.redismq.rpc.cache.RpcGlobalCache;
import com.redismq.rpc.client.AbstractNettyRemoting;
import com.redismq.rpc.manager.NettyClientChannelManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

//单例多线程共享，但线程不安全
@ChannelHandler.Sharable
@Slf4j
public class ClientHandler extends ChannelDuplexHandler {
    
    private final NettyClientChannelManager nettyClientChannelManager;
  
    
    public ClientHandler(NettyClientChannelManager nettyClientChannelManager) {
        this.nettyClientChannelManager = nettyClientChannelManager;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("nettyClient error :" + cause.getLocalizedMessage());
        nettyClientChannelManager.releaseChannel(NetUtil.getAddressFromChannel(ctx.channel()), ctx.channel());
        ctx.close();
    }
    
    /**
     * 在建立连接时发送消息
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
//        log.info("nettyClient :" + channel.remoteAddress() + "  channelActive");
        //连接成功后，向服务端发送消息
        //        ctx.writeAndFlush("[连接server服务成功]:");
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
        if (channel.remoteAddress() == null) {
            return;
        }
        log.info("nettyClient :" + channel.remoteAddress() + " new add");
        //        channel.writeAndFlush("[" + channel.remoteAddress() + "] 加入消息通道连接");
    }
    
    
    /**
     * 处理退出消息通道
     *
     * @param ctx
     * @throws Exception
     */
    //    @Override
    //    public void handlerRemoved(ChannelHandlerContext ctx) {
    //        Channel channel = ctx.channel();
    //        if (channel.remoteAddress()==null){
    //            return;
    //        }
    //        log.info("nettyClient :" + ctx.channel().remoteAddress() + " removed");
    //        nettyClientChannelManager.invalidateObject(getAddressFromChannel(ctx.channel()),ctx.channel());
    //    }
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        log.info("nettyClient channelWritabilityChanged :" + ctx.channel().isWritable());
        synchronized (AbstractNettyRemoting.LOCK) {
            if (ctx.channel().isWritable()) {
                AbstractNettyRemoting.LOCK.notifyAll();
            }
        }
        ctx.fireChannelWritabilityChanged();
    }
    
    
    /**
     * 退出时发送消息
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (!channel.isActive()) {
            log.info("nettyClient :" + channel.remoteAddress() + "is down");
        }
        nettyClientChannelManager.releaseChannel(NetUtil.getAddressFromChannel(ctx.channel()), ctx.channel());
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //设置返回消息
        RemoteMessage remoteMessage = (RemoteMessage) msg;
        RemoteMessageFuture remoteMessageFuture = RpcGlobalCache.REMOTE_FUTURES.remove(remoteMessage.getId());
        if (remoteMessageFuture!=null){
            remoteMessageFuture.setResultMessage(remoteMessage.getBody());
        }
        super.channelRead(ctx, msg);
    }
}