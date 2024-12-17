package com.redismq.common.pojo;

import lombok.Data;

@Data
public class Group {
    /**
     * 分组名
     */
    private String groupName;
    
    /**
     * 最近注册时间
     */
    private Long lastRegisterTime;
}
