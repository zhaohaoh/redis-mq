package com.redismq.server.store;

import com.redismq.common.constant.MessageStatus;
import com.redismq.common.pojo.Message;
import com.redismq.common.serializer.RedisMQStringMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class JdbcStoreStrategy implements MessageStoreStrategy {
    
    private static final String TABLE_NAME = " redismq_message ";
    
    private static final String TABLE_FIELDS = "(id,body,queue,tag,`key`,virtual_queue_name,`offset`,header,status) ";
    
    private final JdbcTemplate jdbcTemplate;
    
    public JdbcStoreStrategy(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    @Override
    public boolean saveMessages(List<Message> messages) {
        List<Object[]> insertSqlParams = new ArrayList<>();
        for (Message message : messages) {
            List<Object> valueList = new ArrayList<>();
    
            String id = message.getId();
            Object body = message.getBody();
            Object key = message.getKey();
            Object queue = message.getQueue();
            Object tag = message.getTag();
            Object header = message.getHeader();
            Object offset = message.getOffset();
            Object virtualQueueName = message.getVirtualQueueName();
            valueList.add(id);
            valueList.add(body);
            valueList.add(queue);
            valueList.add(tag);
            valueList.add(key);
            valueList.add(virtualQueueName);
            valueList.add(offset);
            valueList.add(header==null?"": RedisMQStringMapper.toJsonStr(header));
            valueList.add(MessageStatus.CREATE.getCode());
            insertSqlParams.add(valueList.toArray());
        }
    
     
        String values =
                "values (" + Arrays.stream(TABLE_FIELDS.split(",")).map(a -> "?").collect(Collectors.joining(","))
                        + ")";
        String sql = "insert into" + TABLE_NAME + TABLE_FIELDS + values;
        log.info("create message :{} params:{}",sql,insertSqlParams);
        int[] ints = jdbcTemplate.batchUpdate(sql, insertSqlParams);
        return ints.length>0;
    }
    
    @Override
    public boolean updateStatusByIds(List<String> ids, int status) {
        String values = "(" + String.join(",", ids) + ")";
        String sql = "update " + TABLE_NAME + " set " + " status " + "= " + status + " where id in " + values;
        log.info("update message status :{}",sql);
        int update = jdbcTemplate.update(sql);
        return update>0;
    }
    
    @Override
    public void clearExpireMessage() {
        Date date = new Date();
        date = DateUtils.addMonths(date, -1);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = simpleDateFormat.format(date);
        String deleteSql = "delete from " + TABLE_NAME + "where create_time <=" + format;
        jdbcTemplate.update(deleteSql);
    }
}
