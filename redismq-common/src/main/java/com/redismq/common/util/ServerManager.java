package com.redismq.common.util;

import com.redismq.common.connection.RedisMQServerUtil;
import com.redismq.common.pojo.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * 服务管理者
 */
@Slf4j
public class ServerManager {
    
    private static RedisMQServerUtil redisMQServerUtil;
    
    private static Set<Server> SERVERS = new HashSet<>();
    
    public ServerManager() {
    }
    
    @Autowired
    public void setRedisMQServerUtil(RedisMQServerUtil redisMQServerUtil) {
        ServerManager.redisMQServerUtil = redisMQServerUtil;
    }
    
    public static Set<Server> getLocalAvailServers() {
        return SERVERS;
    }
    
    public static Set<Server> getRemoteAvailServers() {
        Set<Server> servers = redisMQServerUtil.getServers();
        if (CollectionUtils.isEmpty(servers)) {
            return new HashSet<>();
        }
        ServerManager.SERVERS = new HashSet<>(servers);
        return servers;
    }
    
    /**
     * 删除过期的远程服务
     *
     * @param server 服务
     */
    public static void removeExpireServer(Server server) {
        redisMQServerUtil.removeServer(server);
        SERVERS.remove(server);
        log.info("removeExpireServer :{}", server);
    }
    
    /**
     * 添加本地服务
     */
    public static void addLocalServer(Server server) {
        SERVERS.add(server);
    }
    
    /**
     * 添加本地服务
     */
    public static void removeLocalServer(Server server) {
        SERVERS.remove(server);
    }
}
