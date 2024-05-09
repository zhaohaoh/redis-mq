package com.redismq.admin.store;

import com.redismq.common.pojo.Message;

import java.util.List;

public interface MessageStoreStrategy {
    
    boolean updateStatusByIds(List<String> ids,int status);
    
    boolean saveMessages(List<Message> message);
    
    void clearExpireMessage();
}
