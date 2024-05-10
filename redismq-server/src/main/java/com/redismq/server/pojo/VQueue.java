package com.redismq.server.pojo;

import lombok.Data;

/**
 * 虚拟队列
 *
 * @author hzh
 * @date 2023/11/22
 */
@Data
public class VQueue {
    
    /**
     * 队列名称
     */
    private String queueName;
    
    /**
     * 大小
     */
    private Long size;
}
