package com.redismq.rebalance;


import com.redismq.pojo.Client;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static com.redismq.queue.QueueManager.getCurrentVirtualQueues;
import static com.redismq.queue.QueueManager.putCurrentVirtualQueues;

/**
 * @Author: hzh
 * @Date: 2022/4/25 18:18
 * 队列负载均衡相关
 */
public class RebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);
    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    public RebalanceImpl(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    /*
        返回监听的队列
     */
    public void rebalance(List<Client> clients, String clientId) {
        if (CollectionUtils.isEmpty(clients)) {
            log.warn("rebalance get clients is empty");
            return;
        }
        List<String> clientIds = clients.stream().map(Client::getClientId).collect(Collectors.toList());
        List<String> allQueues = QueueManager.getLocalQueues();
        for (String queue : allQueues) {
            List<String> virtualQueues = QueueManager.getLocalVirtualQueues(queue);
            List<String> vQueues = allocateMessageQueueStrategy.allocate(clientId, virtualQueues, clientIds);
            putCurrentVirtualQueues(queue, vQueues);
        }
        String vQueues = getCurrentVirtualQueues().entrySet().stream().map(a -> a.getKey() + ":" + a.getValue()).collect(Collectors.joining("\n"));
        log.info("RebalanceImpl rebalance clientId:{} \n clients:{} \n VirtualQueues:\n{}", clientId, clients,vQueues);
    }

}
