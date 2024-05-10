package com.redismq.rpc.proccess;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.util.RpcMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractMessageProcessor implements RemoteMessageProcessor {
    
    @Override
    public void process(RemoteResponse ctx, List<RemoteMessage> messages) throws Exception {
        doProcess(ctx, messages);
        
        for (RemoteMessage message : messages) {
            RemoteMessage responseMessage = RpcMessageUtil
                    .buildResponseMessage(message.getId(), true, message.getMessageType());
            ctx.setRpcMessage(responseMessage);
            //如果是rpc的话写回通道 否则使用http或者本地
            ChannelHandlerContext channelHandlerContext = ctx.getChannelHandlerContext();
            //这里暂时不会往客户端回复，这里消息是空的
            if (channelHandlerContext != null && ctx.getRpcMessage() != null) {
                boolean uninterruptibly = channelHandlerContext.writeAndFlush(ctx.getRpcMessage())
                        .awaitUninterruptibly(GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout(),
                                TimeUnit.MILLISECONDS);
            }
        }
       
    }
    
    protected abstract void doProcess(RemoteResponse ctx, List<RemoteMessage> messages) throws Exception;
}
