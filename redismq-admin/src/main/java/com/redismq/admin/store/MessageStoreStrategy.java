package com.redismq.admin.store;

import com.redismq.common.pojo.Message;

public interface MessageStoreStrategy {
    
    boolean saveMessage(Message message);
    
    void clearExpireMessage();
}
