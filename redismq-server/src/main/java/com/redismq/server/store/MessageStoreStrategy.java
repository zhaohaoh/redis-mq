package com.redismq.server.store;

import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.QueueGroupOffset;

import java.util.List;

public interface MessageStoreStrategy {
    
    boolean updateStatusByIds(List<String> ids,int status);
    
    boolean saveMessages(List<Message> message);
    
    void clearExpireMessage();
    
    List<Message> getMessageListByQueueAndOffset(QueueGroupOffset offset);
}
