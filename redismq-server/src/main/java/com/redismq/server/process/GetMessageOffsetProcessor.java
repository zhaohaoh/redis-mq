package com.redismq.server.process;

import com.redismq.common.pojo.Message;
import com.redismq.common.pojo.QueueGroupOffset;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.common.util.RpcMessageUtil;
import com.redismq.rpc.proccess.AbstractMessageProcessor;
import com.redismq.server.store.MessageStoreStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static com.redismq.common.constant.MessageType.GET_QUEUE_MESSAGE_BY_OFFSET;

/**
 * 根据偏移量获取消息
 */
@Component
public class GetMessageOffsetProcessor extends AbstractMessageProcessor {
    
    @Autowired
    private MessageStoreStrategy messageStoreStrategy;
    
    @Override
    public boolean doProcess(RemoteResponse remoteResponse, List<RemoteMessage> remoteMessages) {
       
        List<RemoteMessage> response = new ArrayList<>();
        for (RemoteMessage message : remoteMessages) {
            String body = message.getBody();
            QueueGroupOffset bean = RedisMQStringMapper.toBean(body, QueueGroupOffset.class);
            List<Message> messageList = messageStoreStrategy.getMessageListByQueueAndOffset(bean);
            RemoteMessage responseMessage = RpcMessageUtil.buildResponseMessage(message.getId(), messageList,
                    GET_QUEUE_MESSAGE_BY_OFFSET);
            response.add(responseMessage);
        }
        remoteResponse.setRpcMessage(response);
        return true;
    }
    
    @Override
    public Integer getType() {
        return GET_QUEUE_MESSAGE_BY_OFFSET;
    }
}
