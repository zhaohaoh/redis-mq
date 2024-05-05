package com.redismq.admin.process;

import com.redismq.admin.store.MessageStoreStrategy;
import com.redismq.common.constant.MessageType;
import com.redismq.common.pojo.MergedWarpMessage;
import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.rpc.proccess.RemoteMessageProcessor;
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
public class MQMessageProcessor implements RemoteMessageProcessor {
    
    @Autowired
    private MessageStoreStrategy messageStoreStrategy;
    
    @Override
    public void process(RemoteResponse ctx, RemoteMessage remoteMessage) {
        String body = remoteMessage.getBody();
        List<Message> list = new ArrayList<>();
        if (remoteMessage.getMessageType().equals(MessageType.BATCH_MESSAGE)) {
            MergedWarpMessage mergedWarpMessage = RedisMQStringMapper.toBean(body, MergedWarpMessage.class);
            List<RemoteMessage> messages = mergedWarpMessage.getMessages();
            for (RemoteMessage message : messages) {
                Message msg = RedisMQStringMapper.toBean(message.getBody(), Message.class);
                list.add(msg);
            }
        } else {
            //写入db
            Message msg = RedisMQStringMapper.toBean(body, Message.class);
            list.add(msg);
        }
        for (Message message : list) {
            messageStoreStrategy.saveMessage(message);
        }
    }
    
    @Override
    public Integer[] getType() {
        return new Integer[] {MessageType.MESSAGE, MessageType.BATCH_MESSAGE};
    }
}
