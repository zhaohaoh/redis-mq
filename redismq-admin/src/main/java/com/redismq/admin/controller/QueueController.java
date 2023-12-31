package com.redismq.admin.controller;

import com.redismq.Message;
import com.redismq.admin.pojo.MQMessageDTO;
import com.redismq.admin.pojo.VQueue;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.RedisMQConstant;
import com.redismq.queue.Queue;
import com.redismq.utils.RedisMQTemplate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
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
    
    //根据队列名称查询虚拟队列
    @GetMapping("vQueueList")
    public ResponseEntity<List<VQueue>> vQueueList(String queueName,Integer virtual) {
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
    
    /**
     * 指定队列重新拉取消息 TODO前端未接入
     *
     * @return {@link ResponseEntity}<{@link String}>
     */
    @PostMapping("publishPullMessage")
    public ResponseEntity<Void> publishPullMessage(String vQueue) {
        if (vQueue==null){
            throw new RuntimeException();
        }
        redisMQClientUtil.publishPullMessage(vQueue);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    @PostMapping("sendMessage")
    public ResponseEntity<Void> sendMessage(@RequestBody MQMessageDTO message) {
           String queue = RedisMQConstant.getQueueNameByQueue(message.getQueue());
        Message build = Message.builder().body(message.getBody())
                .queue(RedisMQConstant.getQueueNameByVirtual(queue)).tag(message.getTag())
                .virtualQueueName(queue).build();
        redisMQTemplate.sendMessage(build);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    @PostMapping("sendTimingMessage")
    public ResponseEntity<Void> sendTimingMessage(@RequestBody MQMessageDTO message) {
        String queue = RedisMQConstant.getQueueNameByQueue(message.getQueue());
        Message build = Message.builder().body(message.getBody())
                .queue(RedisMQConstant.getQueueNameByVirtual(queue)).tag(message.getTag())
                .virtualQueueName(queue).build();
        redisMQTemplate.sendTimingMessage(build, message.getConsumeTime());
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    
    /**
     * 新增队列 暂时先不接前端
     */
    @GetMapping("addQueue")
    public ResponseEntity<Void> addQueue(Queue queue) {
        //队列名就是topic名
        Queue queue1 = redisMQClientUtil.registerQueue(queue);
    
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    /**
     * 删除队列 暂时先不接前端
     */
    @PostMapping ("deleteQueue")
    public ResponseEntity<Void> deleteQueue(Queue queue) {
        //队列名就是topic名
         redisMQClientUtil.removeQueue(queue);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    
}
