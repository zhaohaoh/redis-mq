package com.redismq.rpc.cache;

import com.redismq.common.pojo.MessageFuture;
import com.redismq.common.pojo.RemoteMessageFuture;

import java.util.concurrent.ConcurrentHashMap;

/**
 * rpc全局缓存
 *
 * @author hzh
 * @date 2024/05/06
 */
public class RpcGlobalCache {
    
    public static final ConcurrentHashMap<String, MessageFuture> FUTURES = new ConcurrentHashMap<>();
    
    public static final ConcurrentHashMap<String, RemoteMessageFuture> REMOTE_FUTURES = new ConcurrentHashMap<>();
    
}
