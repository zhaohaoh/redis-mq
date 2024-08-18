package com.redismq.common.pojo;

import lombok.Data;

import java.util.List;

/**
 * 虚拟队列
 *
 * @author hzh
 * @date 2023/11/22
 */
@Data
public class Client {
    
    /**
     * 客户端名称
     */
    private String clientId;
  
    /**
     * 客户端名称
     */
    private String applicationName;
    
    /**
     * workId
     */
    private Integer workId;
    /**
     * 客户端地址
     */
    private String address;
    /**
     * 客户端监听的队列
     */
    private List<String> queues;
}
