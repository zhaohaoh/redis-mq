package com.redismq.rebalance;


import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.pojo.Client;
import com.redismq.queue.QueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.redismq.queue.QueueManager.getCurrentVirtualQueues;
import static com.redismq.queue.QueueManager.putCurrentVirtualQueues;

/**
 * @Author: hzh
 * @Date: 2022/4/25 18:18
 * 队列负载均衡相关
 */
public class QueueRebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(QueueRebalanceImpl.class);
    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    public QueueRebalanceImpl(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    /**
     * 根据当前 client 列表为本节点重新分配虚拟队列。
     *
     * 这里额外强调“稳定输入 -> 稳定输出”：
     * 1. clientId 必须排序，否则同一批客户端只要心跳返回顺序不同，分配结果就可能抖动；
     * 2. virtualQueue 也必须排序，否则不同节点看到的列表顺序不一致时，会算出不同结果。
     */
    public void rebalance(List<Client> clients, String clientId) {
        if (CollectionUtils.isEmpty(clients)) {
            log.warn("rebalance get clients is empty");
            return;
        }
       
        List<String> allQueues = QueueManager.getLocalQueues();
        for (String queue : allQueues) {
            // 对clientId做稳定排序，避免同一批客户端因为心跳顺序不同导致分配结果抖动。
            List<String> clientIds = clients.stream().filter(c -> c.getQueues() != null && c.getQueues().contains(queue))
                    .filter(a->a.getGroupId().equals(GlobalConfigCache.CONSUMER_CONFIG.getGroupId()))
                    .map(Client::getClientId)
                    .sorted()
                    .collect(Collectors.toList());
            // 对虚拟队列也做稳定排序，确保同样的输入一定得到同样的输出。
            List<String> virtualQueues = new ArrayList<>(QueueManager.getLocalVirtualQueues(queue));
            Collections.sort(virtualQueues);
            List<String> vQueues = allocateMessageQueueStrategy.allocate(clientId, virtualQueues, clientIds);
            putCurrentVirtualQueues(queue, vQueues);
        }
        String vQueues = getCurrentVirtualQueues().entrySet().stream().map(a -> a.getKey() + ":" + a.getValue()).collect(Collectors.joining("\n"));
        log.info("RebalanceImpl rebalance GroupId:{} clientId:{} \n clients:{} \n VirtualQueues:\n{}",
                GlobalConfigCache.CONSUMER_CONFIG.getGroupId(),
                clientId, clients,vQueues);
    }
    
    public static void main(String[] args) {
        AllocateMessageQueueStrategy allocateMessageQueueStrategy1 = new AllocateMessageQueueAveragely();
        List<String> allocate = allocateMessageQueueStrategy1.allocate("v2", Arrays.asList("mq"),
                Arrays.asList("v1", "v2"));
        System.out.println(allocate);
    }

}
