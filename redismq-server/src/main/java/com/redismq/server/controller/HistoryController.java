package com.redismq.server.controller;

import com.redismq.server.pojo.HistoryMessageQueryDTO;
import com.redismq.server.pojo.HistoryMessageVO;
import com.redismq.server.pojo.PageResult;
import com.redismq.server.store.MessageStoreStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/history/message")
public class HistoryController {
    @Autowired
    private MessageStoreStrategy jdbcStoreStrategy;
    /**
     *  消息
     *
     * @return {@link ResponseEntity}<{@link String}>
     */
    @PostMapping("page")
    public ResponseEntity<PageResult<HistoryMessageVO>> page(@RequestBody HistoryMessageQueryDTO message){
        PageResult<HistoryMessageVO> pageResult = jdbcStoreStrategy.pageMessageList(message);
        return ResponseEntity.ok(pageResult);
    }
}
