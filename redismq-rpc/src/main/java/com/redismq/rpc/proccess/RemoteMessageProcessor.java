package com.redismq.rpc.proccess;

import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;

/**
 * rpc消息处理器
 *
 * @author hzh
 * @date 2023/08/15
 */
public interface RemoteMessageProcessor {
    /**
     * 过程
     *
     * @param ctx     ctx
     * @param message 消息
     * @throws Exception 异常
     */
    void process(RemoteResponse ctx, RemoteMessage message) throws Exception;
    
    Integer[] getType();
}
