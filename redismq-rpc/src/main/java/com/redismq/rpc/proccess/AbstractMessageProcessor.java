package com.redismq.rpc.proccess;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.util.RpcMessageUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 抽象消息处理器
 *
 * @author hzh
 * @date 2024/05/27
 */
@Slf4j
public abstract class AbstractMessageProcessor implements RemoteMessageProcessor {
    
    @Override
    public boolean process(RemoteResponse ctx, List<RemoteMessage> messages) throws Exception {
        String exception = null;
        try {
           doProcess(ctx, messages);
        } catch (Exception e) {
            exception = StringUtils.substring(ExceptionUtils.getStackTrace(e), 0, 255);
            log.error("process error messages :{} e:", messages, e);
        }
        for (RemoteMessage message : messages) {
            RemoteMessage responseMessage = RpcMessageUtil
                    .buildResponseMessage(message.getId(), exception == null ? true : exception,
                            message.getMessageType());
            ctx.setRpcMessage(responseMessage);
            //如果是rpc的话写回通道 否则使用http或者本地
            ChannelHandlerContext channelHandlerContext = ctx.getChannelHandlerContext();
            //这里暂时不会往客户端回复，这里消息是空的
            if (channelHandlerContext != null && ctx.getRpcMessage() != null) {
                channelHandlerContext.writeAndFlush(ctx.getRpcMessage())
                        .awaitUninterruptibly(GlobalConfigCache.NETTY_CONFIG.getRpcRequestTimeout(),
                                TimeUnit.MILLISECONDS);
            }
        }
        return true;
    }
    
    protected abstract boolean doProcess(RemoteResponse ctx, List<RemoteMessage> messages) throws Exception;
}
