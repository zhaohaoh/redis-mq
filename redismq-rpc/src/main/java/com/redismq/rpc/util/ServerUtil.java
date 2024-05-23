package com.redismq.rpc.util;

import com.redismq.common.exception.RedisMQRpcException;
import com.redismq.common.pojo.Server;
import com.redismq.common.rebalance.RandomBalance;
import com.redismq.common.rebalance.ServerSelectBalance;
import com.redismq.common.util.ServerManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.redismq.common.config.GlobalConfigCache.PRODUCER_CONFIG;

@Slf4j
public class ServerUtil {
    
    private static final ServerSelectBalance selectBalance = new RandomBalance();
    /**
     * 两个参数暂时无用，预留
     */
    @SuppressWarnings("unchecked")
    public static String loadBalance(String group, Object msg) {
        String address = null;
        int count = 0;
        while (count <=  PRODUCER_CONFIG.loadBalanceRetryCount) {
            count++;
            try {
                Set<Server> servers = ServerManager.getLocalAvailServers();
                if (CollectionUtils.isEmpty(servers)) {
                    Thread.sleep(PRODUCER_CONFIG.loadBalanceRetryMills);
                    continue;
                }
                if (servers.size() == 1) {
                    return servers.iterator().next().getAddress();
                }
                List<String> serverList = servers.stream().map(Server::getAddress).collect(Collectors.toList());
                address = selectBalance.select(serverList, msg.toString());
                if (StringUtils.isNotEmpty(address)) {
                    break;
                }
            } catch (Exception ex) {
                log.error(ex.getMessage());
            }
        }
        if (address == null) {
            throw new RedisMQRpcException("no available server");
        }
        return address;
    }
    
}
