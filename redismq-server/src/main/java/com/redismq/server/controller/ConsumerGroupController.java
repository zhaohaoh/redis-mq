package com.redismq.server.controller;

import com.redismq.common.connection.RedisMQClientUtil;
import com.redismq.common.pojo.Group;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 消费者组管理
 */
@RestController
@RequestMapping("/consumer/group")
public class ConsumerGroupController {
    
    @Autowired
    private RedisMQClientUtil redisMQClientUtil;
    
    /**
     * 消费者组列表
     */
    @GetMapping("list")
    public ResponseEntity<List<Group>> list() {
        Set<String> groups = redisMQClientUtil.getGroups();
        List<Group> groupList = groups.stream().map(a -> {
            Group group = new Group();
            group.setGroupId(a);
            return group;
        }).collect(Collectors.toList());
        return ResponseEntity.ok(groupList);
    }
    
    /**
     * 消费者组删除 此动作高度危险，会删除队列所有消息，对redis有性能消耗
     */
    @DeleteMapping("delete")
    public ResponseEntity  delete(String groupId) {
        redisMQClientUtil.deleteGroup(groupId);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
    
 
}
