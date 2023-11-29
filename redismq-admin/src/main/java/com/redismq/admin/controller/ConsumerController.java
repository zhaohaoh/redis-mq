package com.redismq.admin.controller;

import com.redismq.connection.RedisMQClientUtil;
import com.redismq.pojo.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
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
    
    @GetMapping("down")
    public void down(String clientId) {
        redisMQClientUtil.removeClient(clientId);
    }
}
