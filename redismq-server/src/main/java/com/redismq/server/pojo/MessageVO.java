package com.redismq.server.pojo;

import lombok.Data;

import java.util.Map;

/**
 * mqmessage
 *
 * @author hzh
 * @date 2023/11/20
 */
@Data
public class MessageVO {
    
    /**
     * 消息主体  消息都会转为字符串存储
     */
    private Object body;
    
    /**
     * 消息id
     */
    private String id;
    
    /**
     * 用来路由虚拟队列的key
     */
    private String key="";
    
    /**
     * 队列
     */
    private String queue;
    
    /**
     * 标签
     */
    private String tag = "";
    
    /**
     * 虚拟队列名称 内部生成 外部设置无效
     */
    private String virtualQueueName;
    
    /**
     * 消息头部
     */
    private Map<String,Object> header;
    /**
     * 消费时间
     */
    private String consumeTime;
    /**
     * 消息偏移量
     */
    private Long offset;
    
}
