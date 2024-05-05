package com.redismq.admin.store;

import com.redismq.common.pojo.Message;
import com.redismq.common.serializer.RedisMQStringMapper;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class JdbcStoreStrategy implements MessageStoreStrategy {
    
    private static final String TABLE_NAME = " redismq_message ";
    
    private static final String TABLE_FIELDS = "(id,body,queue,tag,`key`,virtual_queue_name,`offset`,header) ";
    
    private final JdbcTemplate jdbcTemplate;
    
    public JdbcStoreStrategy(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    @Override
    public boolean saveMessage(Message message) {
        String values =
                "values (" + Arrays.stream(TABLE_FIELDS.split(",")).map(a -> "?").collect(Collectors.joining(","))
                        + ")";
        String id = message.getId();
        Object body = message.getBody();
        Object key = message.getKey();
        Object queue = message.getQueue();
        Object tag = message.getTag();
        Object header = message.getHeader();
        Object offset = message.getOffset();
        Object virtualQueueName = message.getVirtualQueueName();
        
        jdbcTemplate
                .update("insert into" + TABLE_NAME + TABLE_FIELDS + values, id, body, queue, tag, key, virtualQueueName,
                        offset, header == null ? "" : RedisMQStringMapper.toJsonStr(header));
        return true;
    }
    
    @Override
    public void clearExpireMessage() {
        Date date = new Date();
        date = DateUtils.addMonths(date,-1);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = simpleDateFormat.format(date);
        String deleteSql = "delete from " + TABLE_NAME + "where create_time <=" + format;
        jdbcTemplate.update(deleteSql);
    }
}
