package com.redismq.core;


import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.pojo.PushMessage;
import com.redismq.common.pojo.Queue;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.queue.QueueManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * @Author: hzh
 * @Date: 2022/5/7 14:16 接受消息订阅
 */
public class RedisPullListener extends AbstractRedisPushListener {
    
    protected static final Logger log = LoggerFactory.getLogger(RedisPullListener.class);
    
    public RedisPullListener(RedisMqClient redisMqClient) {
        super(redisMqClient);
    }
    
    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            semaphore.acquire();
            byte[] body = message.getBody();
            PushMessage pushMessage = RedisMQStringMapper.toBean(body, PushMessage.class);
            String queueName = pushMessage.getQueue();
            String vQueueName = StringUtils.substringBetween(queueName, "{", "}");
            String realNameQueue = RedisMQConstant.getQueueNameByVirtual(vQueueName);
            pushMessage.setQueue(vQueueName);
            //当前服务订阅的队列列表
            List<String> list = QueueManager.getCurrentVirtualQueues().get(realNameQueue);
            if (list == null || !list.contains(vQueueName)) {
                return;
            }
            Queue queue = QueueManager.getQueue(realNameQueue);
            if (queue == null) {
                return;
            }
            boolean delayState = queue.isDelayState();
            
            //延时队列和普通队列分开处理
            if (delayState) {
                LinkedBlockingQueue<PushMessage> delayBlockingQueue = redisMqClient.getRedisListenerContainerManager()
                        .getDelayBlockingQueue();
                // 延时的时间小于当前时间，并且消息中已经存在比当前时间小的延时消息
                Optional<PushMessage> first = delayBlockingQueue.stream()
                        .filter(a -> a.getTimestamp() <= System.currentTimeMillis()).findFirst();
                if (first.isPresent()){
                    if (pushMessage.getTimestamp()<=System.currentTimeMillis()){
                        return;
                    }
                }
                if (!delayBlockingQueue.contains(pushMessage)) {
                    delayBlockingQueue.add(pushMessage);
                }
            } else {
                LinkedBlockingQueue<String> linkedBlockingQueue = redisMqClient.getRedisListenerContainerManager()
                        .getLinkedBlockingQueue();
                if (!linkedBlockingQueue.contains(vQueueName)) {
                    linkedBlockingQueue.add(vQueueName);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release();
        }
    }
    
}
