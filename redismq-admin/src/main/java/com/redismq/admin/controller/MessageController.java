package com.redismq.admin.controller;

import com.redismq.Message;
import com.redismq.admin.pojo.MQMessageQueryDTO;
import com.redismq.connection.RedisMQClientUtil;
import com.redismq.constant.RedisMQConstant;
import javafx.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/message")
public class MessageController {
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    /**
     * 分页查询消息
     *
     * @return {@link ResponseEntity}<{@link String}>
     */
    @PostMapping("page")
    public ResponseEntity<List<Message>> page(@RequestBody MQMessageQueryDTO mqMessageDTO){
        String vQueue = mqMessageDTO.getVirtualQueueName();
        vQueue= RedisMQConstant.getQueueNameByQueue(vQueue);
        List<Pair<Message, Double>> pairs = redisMQClientUtil.pullMessageWithScope(vQueue,mqMessageDTO.getStartOffset(),mqMessageDTO.getSize());
        List<Message> messages = pairs.stream().map(Pair::getKey).collect(Collectors.toList());
        return ResponseEntity.ok(messages);
    }
    
    /**
     * 删除消息
     *
     * @return {@link ResponseEntity}<{@link String}>
     */
    @PostMapping("deleteMessage")
    public ResponseEntity deleteMessage(@RequestBody Message message){
        message.setVirtualQueueName(RedisMQConstant.getQueueNameByQueue(message.getQueue()));
        Long aLong = redisMQClientUtil.removeMessage(message.getQueue(), message);
        return ResponseEntity.ok(aLong);
    }
    
}
