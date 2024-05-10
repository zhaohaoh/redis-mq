package com.redismq.admin.process;

import com.redismq.admin.store.MessageStoreStrategy;
import com.redismq.common.constant.MessageStatus;
import com.redismq.common.constant.MessageType;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
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
public class SendMessageSuccessProcessor extends AbstractMessageProcessor {
    
    @Autowired
    private MessageStoreStrategy messageStoreStrategy;
    
    @Override
    public void doProcess(RemoteResponse ctx, List<RemoteMessage> remoteMessages) {
        
        List<String> ids = new ArrayList<>();
        for (RemoteMessage message : remoteMessages) {
            String body = message.getBody();
            ids.add(body);
        }
        
        messageStoreStrategy.updateStatusByIds(ids, MessageStatus.SUCCESS.getCode());
    }
    
    @Override
    public Integer getType() {
        return MessageType.SEND_MESSAGE_SUCCESS;
    }
}
