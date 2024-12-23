package com.redismq.common.pojo;

import io.netty.channel.ChannelHandlerContext;
import lombok.Data;

import java.util.List;

/**
 *  返回体
 *
 * @author hzh
 * @date 2023/08/15
 */
@Data
public class RemoteResponse {
    /**
     * 通道处理程序上下文
     */
    private ChannelHandlerContext channelHandlerContext;
    /**
     * 返回的rpc消息
     */
    private List<RemoteMessage> rpcMessage;
}
