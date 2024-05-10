package com.redismq.common.util;


import com.redismq.common.pojo.AddressInfo;
import com.redismq.common.pojo.RemoteMessage;
import com.redismq.common.serializer.RedisMQStringMapper;

import java.util.UUID;

public class RpcMessageUtil {
    public static RemoteMessage buildRequestMessage(Object msg, AddressInfo addressInfo, Integer messageType) {
        RemoteMessage rpcMessage = new RemoteMessage();
        rpcMessage.setId(UUID.randomUUID().toString());
        rpcMessage.setMessageType(messageType);
        rpcMessage.setBody(RedisMQStringMapper.toJsonStr(msg));
        rpcMessage.setAddressInfo(addressInfo);
        return rpcMessage;
    }

    public static RemoteMessage buildResponseMessage(String id,Object responseMsg ,Integer messageType) {
        RemoteMessage rpcMessage = new RemoteMessage();
        rpcMessage.setMessageType(messageType);
        rpcMessage.setBody(RedisMQStringMapper.toJsonStr(responseMsg));
        rpcMessage.setId(id);
        return rpcMessage;
    }
}
