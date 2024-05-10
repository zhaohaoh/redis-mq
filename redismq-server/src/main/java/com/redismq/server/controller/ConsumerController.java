package com.redismq.server.controller;

import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.pojo.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;


@RestController
@RequestMapping("/consumer")
public class ConsumerController {
    
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    /**
     * 消费者客户端列表
     */
    @GetMapping("list")
    public ResponseEntity<List<Client>> list() {
        List<Client> clientsWithTime = redisMQClientUtil.getClients();
        
        return ResponseEntity.ok(clientsWithTime);
    }
    
    /**
     * 强制下线客户端 还没做拉黑,就算下线了该客户端还会自动注册
     */
    @GetMapping("down")
    public void down(String clientId) {
        redisMQClientUtil.removeClient(clientId);
    }
    
    /**
     * 强制所有消费者重平衡 TODO前端未接入
     */
    @PutMapping("rebalance")
    public void rebalance() {
        redisMQClientUtil.publishRebalance("redisMQAdminClient");
    }
}
