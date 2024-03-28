package com.redismq.admin.controller;

import com.alibaba.fastjson.JSONValidator;
import com.redismq.Message;
import com.redismq.admin.pojo.MQMessageDTO;
import com.redismq.admin.pojo.PageResult;
import com.redismq.admin.pojo.QueuePageSelect;
import com.redismq.admin.pojo.VQueue;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.RedisMQConstant;
import com.redismq.queue.Queue;
import com.redismq.utils.JsonSerializerUtil;
import com.redismq.utils.RedisMQTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.constant.GlobalConstant.SPLITE;


@RestController
@RequestMapping("/queue")
@Slf4j
public class QueueController {
    
    @Autowired
    private RedisMQTemplate redisMQTemplate;
    
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    @GetMapping("page")
    public ResponseEntity<PageResult<Queue>> page(QueuePageSelect queuePageSelect) {
        //队列名就是topic名
       Set<Queue> allQueue = redisMQClientUtil.getQueueList();
       
        allQueue=allQueue.stream().filter(a->a.isDelayState()==queuePageSelect.isDelayState()).collect(Collectors.toSet());
    
        int start = (queuePageSelect.getPage() - 1) * queuePageSelect.getSize();
        int end = queuePageSelect.getPage() * queuePageSelect.getSize();
        List<Queue> page = limitPage(new ArrayList<>(allQueue), start, end);
        return ResponseEntity.ok(PageResult.success(allQueue.size(),page));
    }
    
    //根据队列名称查询虚拟队列
    @GetMapping("vQueueList")
    public ResponseEntity<List<VQueue>> vQueueList(String queueName, Integer virtual) {
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
        if (vQueue == null) {
            throw new RuntimeException();
        }
        redisMQClientUtil.publishPullMessage(vQueue);
        log.info("publishPullMessage :{}",vQueue);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    @PostMapping("sendMessage")
    public ResponseEntity<Boolean> sendMessage(@RequestBody MQMessageDTO message) {
        Object body = message.getBody();
        if (body instanceof String){
            String str = (String) body;
            if (JsonSerializerUtil.isJson(str)){
                boolean validate = JSONValidator.from(str).validate();
                if (!validate) {
                    return  ResponseEntity.ok(false);
                }
            }
        }
        
        Message build = Message.builder().body(message.getBody()).queue(RedisMQConstant.getQueueNameByVirtual(message.getQueue()))
                .tag(message.getTag()).virtualQueueName(message.getQueue()).build();
        redisMQTemplate.sendMessage(build);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    @PostMapping("sendTimingMessage")
    public ResponseEntity<Boolean> sendTimingMessage(@RequestBody MQMessageDTO message) {
        Object body = message.getBody();
        if (body instanceof String){
            String str = (String) body;
            if (JsonSerializerUtil.isJson(str)){
                boolean validate = JSONValidator.from(str).validate();
                if (!validate) {
                    return  ResponseEntity.ok(false);
                }
            }
        }
        String queue = message.getQueue();
        Message build = Message.builder().body(message.getBody()).queue(RedisMQConstant.getQueueNameByVirtual(queue))
                .tag(message.getTag()).virtualQueueName(queue).build();
        redisMQTemplate.sendTimingMessage(build, message.getConsumeTime());
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    
    /**
     * 新增队列 暂时先不接前端
     */
    @PostMapping("addQueue")
    public ResponseEntity<Void> addQueue(@RequestBody Queue queue) {
        //队列名就是topic名
        Queue queue1 = redisMQClientUtil.registerQueue(queue);
        
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    /**
     * 删除队列 暂时先不接前端
     */
    @DeleteMapping("deleteQueue")
    public ResponseEntity<Void> deleteQueue(String queue) {
        //队列名就是topic名
        Queue removeQueue = redisMQClientUtil.getQueue(RedisMQConstant.getVQueueNameByVQueue(queue));
        redisMQClientUtil.removeQueue(removeQueue);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
    public static <T> List<T> limitPage(List<T> list, int start, int end) {
        if (list.size() < end) {
            list = list.subList(start, list.size());
        } else {
            list = list.subList(start, end);
        }
        return list;
    }
    
    
}
