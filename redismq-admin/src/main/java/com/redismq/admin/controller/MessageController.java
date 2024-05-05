package com.redismq.admin.controller;

import com.redismq.admin.pojo.MQMessageQueryDTO;
import com.redismq.admin.pojo.MessageVO;
import com.redismq.admin.pojo.PageResult;
import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.pojo.Message;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
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
    public ResponseEntity<PageResult<MessageVO>> page(@RequestBody MQMessageQueryDTO mqMessageDTO){
        String vQueue = mqMessageDTO.getVirtualQueueName();
        vQueue= RedisMQConstant.getVQueueNameByVQueue(vQueue);
        Long total = redisMQClientUtil.queueSize(vQueue);
        List<Pair<Message, Double>> pairs = redisMQClientUtil.pullMessageWithScope(vQueue,mqMessageDTO.getStartOffset(),mqMessageDTO.getEndOffset());
        List<MessageVO> messages = pairs.stream().map(m->{
            MessageVO messageVO = new MessageVO();
            BeanUtils.copyProperties(m.getKey(),messageVO);
            Date date = new Date(m.getValue().longValue());
            String format = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss");
            messageVO.setConsumeTime(format);
            return messageVO;
        }).collect(Collectors.toList());
      
        return ResponseEntity.ok(PageResult.success(total,messages));
    }
    
    /**
     * 删除消息
     *
     * @return {@link ResponseEntity}<{@link String}>
     */
    @PostMapping("deleteMessage")
    public ResponseEntity deleteMessage(@RequestBody Message message){
        Boolean success = redisMQClientUtil.removeMessage(message.getVirtualQueueName(), message.getId());
        return ResponseEntity.ok(success);
    }
    
}
