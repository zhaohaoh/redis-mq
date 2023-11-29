package com.redismq.admin.controller;

import com.redismq.Message;
import com.redismq.admin.pojo.MQMessageDTO;
import com.redismq.admin.pojo.VQueue;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.queue.Queue;
import com.redismq.utils.RedisMQTemplate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.redismq.constant.GlobalConstant.SPLITE;
import static com.redismq.constant.RedisMQConstant.NAMESPACE;
import static com.redismq.constant.RedisMQConstant.PREFIX;


@RestController
@RequestMapping("/queue")
public class QueueController {
    
    @Autowired
    private RedisMQTemplate redisMQTemplate;
    
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    @GetMapping("page")
    public ResponseEntity<Set<Queue>> page() {
        //队列名就是topic名
        Set<Queue> allQueue = redisMQClientUtil.getQueueList();
        allQueue.forEach((a -> {
            a.setQueueName(StringUtils.substringAfterLast(a.getQueueName(), PREFIX + NAMESPACE + SPLITE));
        }));
        return ResponseEntity.ok(allQueue);
    }
    
    //根据topic名称查询虚拟队列
    @GetMapping("vQueueList")
    public ResponseEntity<List<VQueue>> vQueueList(String queueName,Integer virtual) {
//        Set<Queue> allQueue = redisMQClientUtil.getQueueList();
//        Optional<Queue> first = allQueue.stream().filter(a -> a.getQueueName().equals(queueName)).findFirst();
//        Queue queue = first.get();
//        Integer virtual = queue.getVirtual();
        List<VQueue> virtualQueues = new ArrayList<>();
        for (int i = 0; i < virtual; i++) {
            VQueue vQueue = new VQueue();
            String virtualQueue = queueName + SPLITE + i;
            Long size = redisMQClientUtil.queueSize(virtualQueue);
            vQueue.setQueueName(virtualQueue);
            vQueue.setSize(size == null ? 0 : size);
            virtualQueues.add(vQueue);
        }
        
        return ResponseEntity.ok(virtualQueues);
    }
    
    @PostMapping("sendMessage")
    public ResponseEntity<String> sendMessage(@RequestBody MQMessageDTO message) {
        Message build = Message.builder().body(message.getBody())
                .queue(message.getQueue()).tag(message.getTag()).build();
        redisMQTemplate.sendMessage(build);
        return ResponseEntity.ok(null);
    }
    
    @PostMapping("sendTimingMessage")
    public ResponseEntity<String> sendTimingMessage(@RequestBody MQMessageDTO message) {
        Message build = Message.builder().body(message.getBody()).tag(message.getTag()).build();
        redisMQTemplate.sendTimingMessage(build, message.getConsumeTime());
        return ResponseEntity.ok(null);
    }
    
}
