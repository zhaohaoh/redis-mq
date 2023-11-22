package com.redismq.admin.controller;

import com.redismq.connection.RedisMQClientUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;


@RestController
@RequestMapping("/consumer")
public class ConsumerController {
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    /**
     * 消费者客户端列表
     */
    @GetMapping("list")
    public ResponseEntity<Set<String>> list(){
        Set<String> client = redisMQClientUtil.getClients();
        return ResponseEntity.ok(client);
    }
}
