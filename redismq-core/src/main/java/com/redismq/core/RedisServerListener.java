package com.redismq.core;


import com.redismq.common.pojo.Server;
import com.redismq.common.pojo.ServerRegisterInfo;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.common.util.ServerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;


/**
 * @Author: hzh
 * 服务端监听
 */
public class RedisServerListener implements MessageListener {
    
    protected static final Logger log = LoggerFactory.getLogger(RedisServerListener.class);
    
    @Override
    public void onMessage(Message message, byte[] pattern) {
        String registerJson = message.toString();
        ServerRegisterInfo serverRegisterInfo = RedisMQStringMapper.toBean(registerJson, ServerRegisterInfo.class);
        Server server = new Server();
        server.setAddress(serverRegisterInfo.getAddress());
        if (serverRegisterInfo.isRegister()){
            ServerManager.addLocalServer(server);
        }else{
            ServerManager.removeLocalServer(server);
        }
       
    }
    
}
