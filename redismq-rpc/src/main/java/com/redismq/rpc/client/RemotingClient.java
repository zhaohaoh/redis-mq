package com.redismq.rpc.client;

import java.util.List;

public interface RemotingClient {
    Object sendSync(Object msg,int messageType);
    
    Object sendBatchSync(List<?> msg,int messageType);
    
    void sendAsync(Object message,int messageType);
 
 
}
