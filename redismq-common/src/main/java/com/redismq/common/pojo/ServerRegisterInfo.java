package com.redismq.common.pojo;

import lombok.Data;

@Data
public class ServerRegisterInfo {
    /**
     * 客户端地址
     */
    private String address;
    /**
     * 是否注册信息
     */
    private boolean register;
}
