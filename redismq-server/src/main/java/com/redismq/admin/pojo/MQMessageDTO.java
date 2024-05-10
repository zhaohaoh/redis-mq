package com.redismq.admin.pojo;

import lombok.Data;

/**
 * mqmessage
 *
 * @author hzh
 * @date 2023/11/20
 */
@Data
public class MQMessageDTO {
    
    /**
     * 消息主体
     */
    private Object body;
    
    /**
     * 主题
     */
    private String queue;
    
    /**
     * 标签
     */
    private String tag = "";
    /**
     * 消费时间
     */
    private Long consumeTime;
}
