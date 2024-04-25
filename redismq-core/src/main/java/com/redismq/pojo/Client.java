package com.redismq.pojo;

import lombok.Data;

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
}
