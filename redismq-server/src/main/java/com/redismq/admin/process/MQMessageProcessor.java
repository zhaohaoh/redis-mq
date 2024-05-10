package com.redismq.admin.process;

import com.redismq.admin.store.MessageStoreStrategy;
import com.redismq.common.constant.MessageType;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.serializer.RedisMQStringMapper;

import com.redismq.rpc.proccess.AbstractMessageProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * MQ发送消息处理器
 *
 * @author hzh
 * @date 2024/04/30
 */
@Component
public class MQMessageProcessor extends AbstractMessageProcessor {
    
    @Autowired
    private MessageStoreStrategy messageStoreStrategy;
    
    @Override
    public void doProcess(RemoteResponse ctx, List<RemoteMessage> remoteMessages) {
        
        List<Message> list = new ArrayList<>();
        for (RemoteMessage message : remoteMessages) {
            Message msg = RedisMQStringMapper.toBean(message.getBody(), Message.class);
            list.add(msg);
        }
        
     messageStoreStrategy.saveMessages(list);
    }
    
    @Override
    public Integer getType() {
        return MessageType.CREATE_MESSAGE;
    }
}
