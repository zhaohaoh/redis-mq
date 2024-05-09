package com.redismq.rpc.proccess;


import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteMessageFuture;
import com.redismq.common.pojo.RemoteResponse;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.redismq.rpc.cache.RpcGlobalCache.REMOTE_FUTURES;

/**
 * rpc消息处理过程
 *
 * @author hzh
 * @date 2023/08/15
 */
@Slf4j
public class RemoteServerProccessManager {
    
    final ThreadPoolExecutor messageExecutor = new ThreadPoolExecutor(
            Math.max(Runtime.getRuntime().availableProcessors() * 4, 16),
            Math.max(Runtime.getRuntime().availableProcessors() * 8, 32), 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(2000), new ThreadFactory() {
        private final ThreadGroup group;
        
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        
        private static final String NAME_PREFIX = "message-default-proccess";
        
        {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        }
        
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, NAME_PREFIX + threadNumber.getAndIncrement());
            t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }, new ThreadPoolExecutor.CallerRunsPolicy());
    
    /**
     * 处理器
     */
    public static final HashMap<Integer/*MessageType*/, Pair<RemoteMessageProcessor, ExecutorService>> PROCESSOR_TABLE = new HashMap<>(
            8);
    
    
    public void registerProcessor(RemoteMessageProcessor remoteMessageProcessor) {
        //循环添加远程消息处理器  先用同步线程
        PROCESSOR_TABLE.put(remoteMessageProcessor.getType(), new Pair<>(remoteMessageProcessor, null));
    }
    
    /**
     * 处理消息
     */
    public void processMessage(RemoteResponse response, RemoteMessage remoteMessage) {
        //策略模式使用指定的策略取执行rpc消息
        Pair<RemoteMessageProcessor, ExecutorService> pair;
        
        pair = PROCESSOR_TABLE.get(remoteMessage.getMessageType());
        
        if (pair != null) {
            if (pair.getValue() != null) {
                try {
                    pair.getValue().execute(() -> {
                        try {
                            pair.getKey().process(response, Collections.singletonList(remoteMessage));
                        } catch (Throwable th) {
                            log.error(th.getMessage(), th);
                        } finally {
                            MDC.clear();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    log.error("ExecutorService RejectedExecutionException", e);
                }
            } else {
                try {
                    pair.getKey().process(response, Collections.singletonList(remoteMessage));
                } catch (Throwable th) {
                    log.error(th.getMessage(), th);
                }
            }
        }
        
        RemoteMessageFuture remoteMessageFuture = REMOTE_FUTURES.get(remoteMessage.getId());
        if (remoteMessageFuture!=null) {
            remoteMessageFuture.setResultMessage(true);
        }
        //        //如果是rpc的话写回通道 否则使用http或者本地
        //        ChannelHandlerContext channelHandlerContext = response.getChannelHandlerContext();
        //        //这里暂时不会往客户端回复，这里消息是空的
        //        if (channelHandlerContext != null && response.getRpcMessage() !=null ) {
        //            channelHandlerContext.writeAndFlush(response.getRpcMessage())
        //                    .awaitUninterruptibly(30000, TimeUnit.MILLISECONDS);
        //        }
    }
}
