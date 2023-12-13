package com.redismq.admin.pojo;

import lombok.Data;

/**
 * mqmessage
 *
 * @author hzh
 * @date 2023/11/20
 */
@Data
public class MQMessageQueryDTO {
    /**
     * 虚拟队列名
     */
    private String virtualQueueName;
   
    /**
     * 消费开始时间
     */
    private Long beginTime;
    
    /**
     * 消费结束时间
     */
    private Long endTime;
    
    private Integer page = 1;
    
    private Integer size = 10;
    
    public Integer getStartOffset() {
        return (page -1)* size;
    }
   
}
