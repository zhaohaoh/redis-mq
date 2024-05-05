package com.redismq.rpc.manager;

import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

@Slf4j
public class ChannelGroupManger  {
    
    private static final ChannelGroup GROUP = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);


    public static String getAddress(Channel channel) {
        InetSocketAddress ipSocket = (InetSocketAddress) (channel.remoteAddress());
        return ipSocket.getAddress().getHostAddress();
    }

    public static void add(ChannelHandlerContext channelHandlerContext) {
        GROUP.add(channelHandlerContext.channel());
    }


    public static String getSnByChannelId(ChannelHandlerContext ctx) {
        return (String) ctx.channel().attr(AttributeKey.valueOf("sn")).get();
    }

    public static void removeChannelHandlerContext(ChannelHandlerContext ctx) {
        GROUP.remove(ctx.channel());
    }


}
