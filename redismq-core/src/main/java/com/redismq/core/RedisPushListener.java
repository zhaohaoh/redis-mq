package com.redismq.core;

import com.redismq.constant.PushMessage;
import com.redismq.queue.Queue;
import com.redismq.queue.QueueManager;
import com.redismq.utils.RedisMQObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static com.redismq.constant.QueueConstant.SPLITE;


/**
 * @Author: hzh
 * @Date: 2022/5/7 14:16
 * 接受消息订阅
 */
public class RedisPushListener extends AbstractRedisPushListener {
    protected static final Logger log = LoggerFactory.getLogger(RedisPushListener.class);

    public RedisPushListener(RedisMqClient redisMqClient) {
        super(redisMqClient);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            semaphore.acquire();
            byte[] body = message.getBody();
            PushMessage pushMessage = RedisMQObjectMapper.toBean(body, PushMessage.class);
            String queueName = pushMessage.getQueue();
            String realNameQueue = StringUtils.substringBeforeLast(queueName, SPLITE);
            //当前服务订阅的队列列表
            List<String> list = QueueManager.CURRENT_VIRTUAL_QUEUES.get(realNameQueue);
            log.debug("RedisPushListener onMessage:{} Queue:{} currentVirtualQueues:{}", pushMessage, realNameQueue, list);
            if (list == null || !list.contains(queueName)) {
                return;
            }
            Queue queue = QueueManager.getQueue(realNameQueue);
            if (queue == null) {
                return;
            }
            boolean delayState = queue.getDelayState();

            //延时队列和普通队列分开处理
            if (delayState) {
                LinkedBlockingQueue<PushMessage> delayBlockingQueue = redisMqClient.getRedisListenerContainerManager().getDelayBlockingQueue();
                if (!delayBlockingQueue.contains(pushMessage)) {
                    try {
                        delayBlockingQueue.add(pushMessage);
                    } catch (Exception e) {
                        log.error("queue full");
                        throw e;
                    }
                }
            } else {
                LinkedBlockingQueue<String> linkedBlockingQueue = redisMqClient.getRedisListenerContainerManager().getLinkedBlockingQueue();
                if (!linkedBlockingQueue.contains(queueName)) {
                    linkedBlockingQueue.add(queueName);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release();
        }
    }

}
