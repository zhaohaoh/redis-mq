package com.redismq.server.process;

import com.redismq.common.constant.MessageType;
import com.redismq.common.pojo.MergedRemoteMessage;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.pojo.RemoteResponse;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.rpc.proccess.AbstractMessageProcessor;
import com.redismq.rpc.proccess.RemoteMessageProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static com.redismq.rpc.proccess.RemoteServerProccessManager.PROCESSOR_TABLE;

/**
 * 批处理rpc消息处理器  对批量的消息进行消息分发
 *
 * @author hzh
 * @date 2024/05/27
 */
@Component
@Slf4j
public class BatchRpcMessageProcessor extends AbstractMessageProcessor {
    
    @Override
    public boolean doProcess(RemoteResponse ctx, List<RemoteMessage> messages) throws Exception {
        RemoteMessage remoteMessage = messages.get(0);
        MergedRemoteMessage mergedRemoteMessage = RedisMQStringMapper
                .toBean(remoteMessage.getBody(), MergedRemoteMessage.class);
        Map<Integer, List<RemoteMessage>> typeMap = mergedRemoteMessage.getMessages().stream()
                .collect(Collectors.groupingBy(RemoteMessage::getMessageType));
        Collection<List<RemoteMessage>> values = typeMap.values();
        
        for (List<RemoteMessage> value : values) {
            Integer messageType = value.get(0).getMessageType();
            Pair<RemoteMessageProcessor, ExecutorService> servicePair = PROCESSOR_TABLE.get(messageType);
            ExecutorService executorService = servicePair.getValue();
            if (executorService != null) {
                try {
                     executorService.submit(() -> {
                        try {
                            servicePair.getKey().process(ctx, value);
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
                RemoteMessageProcessor key = servicePair.getKey();
                key.process(ctx, value); // 这里只处理第一个，因为批量消息都是同一个请求
            }
        }
         return true;
    }
   
    
    @Override
    public Integer getType() {
        return MessageType.BATCH_MESSAGE;
    }
}
