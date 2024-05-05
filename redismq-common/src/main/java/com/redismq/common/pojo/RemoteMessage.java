package com.redismq.common.pojo;

import lombok.Data;

@Data
public class RemoteMessage {
    /**
     * id
     */
    private String id;
    /**
     * 消息类型
     */
    private Integer messageType;
    /**
     * 消息体
     */
    private String body;
    /**
     * 地址信息
     */
    private AddressInfo addressInfo;
}
