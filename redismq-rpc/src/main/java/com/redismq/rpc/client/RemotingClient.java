package com.redismq.rpc.client;

import com.redismq.common.pojo.MergedWarpMessage;

import java.util.concurrent.TimeoutException;

public interface RemotingClient {
    
    Object sendSync(Object msg) throws TimeoutException;
    
    void sendAsync(Object remoteMessage);
    
    void sendBatchAsync(String serverAddress, MergedWarpMessage remoteMessage);
}
