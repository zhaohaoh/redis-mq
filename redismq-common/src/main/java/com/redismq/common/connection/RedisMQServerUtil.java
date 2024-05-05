package com.redismq.common.connection;


import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.pojo.Server;
import com.redismq.common.pojo.ServerRegisterInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.common.constant.RedisMQConstant.getServerCollection;
import static com.redismq.common.constant.RedisMQConstant.getServerTopic;

@Slf4j
public class RedisMQServerUtil {
    
    private final RedisClient redisClient;
    
    public RedisMQServerUtil(RedisClient redisClient) {
        this.redisClient = redisClient;
    }
    
    
    public void registerServer(Server server) {
        long heartbeatTime = System.currentTimeMillis();
        redisClient.zAdd(getServerCollection(), server,heartbeatTime);
    }
    
    public void publishServer(ServerRegisterInfo server) {
        redisClient.convertAndSend(getServerTopic(),server);
    }
    
    public void removeServer(Server server) {
        redisClient.zRemove(getServerCollection(),server);
    }
  
    /**
     * 所有服务端
     *
     * @return {@link Set}<{@link String}>
     */
    public Set<Server> getServers() {
        String serverCollection = getServerCollection();
        Map<Server, Double> serverMap = redisClient
                .zRangeWithScores(serverCollection, 0, Long.MAX_VALUE, Server.class);
        if (CollectionUtils.isEmpty(serverMap)) {
            return new HashSet<>();
        }
        int serverRegisterExpireSeconds = GlobalConfigCache.NETTY_CONFIG.getServer().getServerRegisterExpireSeconds();
        int removeTime = (serverRegisterExpireSeconds << 1) * 1000;
        serverMap.entrySet().removeIf((e)->{
            Double value = e.getValue();
            long longValue = value.longValue();
            long expireTime = System.currentTimeMillis() - longValue;
            //超过10秒说明是无效的直接删除
            if (expireTime > removeTime){
                redisClient.zRemove(serverCollection,e.getKey());
                return true;
            }
            return false;
        });
        
        return serverMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    }
    

}
