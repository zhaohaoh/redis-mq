package com.redismq.server.store;

import com.redismq.common.config.GlobalConfigCache;
import com.redismq.common.constant.MessageStatus;
import com.redismq.common.pojo.GroupOffsetQeueryMessageDTO;
import com.redismq.common.pojo.Message;
import com.redismq.common.serializer.RedisMQStringMapper;
import com.redismq.server.pojo.HistoryMessageQueryDTO;
import com.redismq.server.pojo.HistoryMessageVO;
import com.redismq.server.pojo.PageResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class JdbcStoreStrategy implements MessageStoreStrategy {
    
    private static final String TABLE_NAME = " redismq_message ";
    
    private static final String TABLE_FIELDS = "(id,body,queue,tag,`key`,virtual_queue_name,`offset`,header,status,executor_scope,execute_time) ";
    
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
            Long executeScope = message.getExecuteScope();
            Long executeTime = message.getExecuteTime();
            valueList.add(id);
            valueList.add(body);
            valueList.add(queue);
            valueList.add(tag);
            valueList.add(key);
            valueList.add(virtualQueueName);
            valueList.add(offset);
            valueList.add(header==null?"": RedisMQStringMapper.toJsonStr(header));
            valueList.add(MessageStatus.CREATE.getCode());
            valueList.add(executeScope);
            valueList.add(executeTime);
            insertSqlParams.add(valueList.toArray());
        }
    
     
        String values =
                "values (" + Arrays.stream(TABLE_FIELDS.split(",")).map(a -> "?").collect(Collectors.joining(","))
                        + ")";
        String sql = "insert ignore into " + TABLE_NAME + TABLE_FIELDS + values;
        log.info("create message :{} params:{}",sql,insertSqlParams);
        int[] ints = jdbcTemplate.batchUpdate(sql, insertSqlParams);
        return ints.length>0;
    }
    
    @Override
    public boolean updateStatusByIds(List<String> ids, int status) {
        ids = ids.stream().map(s -> "'" + s + "'").collect(Collectors.toList());
        String values = "(" + String.join(",", ids) + ")";
        String sql = "update " + TABLE_NAME + " set " + " status " + "= " + status + " where id in " + values;
        log.info("update message status SQL :{}",sql);
        int update = jdbcTemplate.update(sql);
        return update>0;
    }
    
    @Override
    public void clearExpireMessage() {
        Date date = new Date();
        long days = GlobalConfigCache.GLOBAL_STORE_CONFIG.getExpireTime().toDays();
        date = DateUtils.addDays(date, (int) -days);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = simpleDateFormat.format(date);
        String deleteSql = "delete from " + TABLE_NAME + "where create_time <=" + format;
        log.info("clearExpireMessage sql:{}",deleteSql);
        jdbcTemplate.update(deleteSql);
    }
    
    @Override
    public List<Message> getMessageListByQueueAndOffset(GroupOffsetQeueryMessageDTO queueGroupOffset) {
        String vQueue = queueGroupOffset.getVQueue();
        Long offset = queueGroupOffset.getOffset();
        Long lastOffset = queueGroupOffset.getLastOffset();
        String sql = "select * from "+TABLE_NAME+" where virtual_queue_name ="+"'"+vQueue+"'"+" and  `offset` >"+offset +" and  `offset` <=" + lastOffset
                +" ORDER BY `offset` limit 100";
        log.info("getMessageListByQueueAndOffset sql:{}",sql);
        List<Map<String, Object>> query = jdbcTemplate.queryForList(sql);
        List<Message> messageList=new ArrayList<>();
        for (Map<String, Object> map : query) {
            Message message = new Message();
            message.setQueue((String)map.get("queue"));
            message.setBody(map.get("body"));
            message.setId((String)map.get("id"));
            message.setTag((String)map.get("tag"));
            message.setKey((String)map.get("key"));
            message.setVirtualQueueName((String)map.get("virtual_queue_name"));
            message.setOffset(Long.parseLong(map.get("offset").toString()));
            message.setExecuteTime(Long.parseLong(map.get("execute_time").toString()));
            message.setExecuteScope( Long.parseLong(map.get("executor_scope").toString()));
            message.setHeader(StringUtils.isNotBlank(map.get("header").toString()) ?RedisMQStringMapper.toMap(map.get("header").toString()):null);
            messageList.add(message);
        }
        return messageList;
    }
    
    @Override
    public PageResult<HistoryMessageVO> pageMessageList(HistoryMessageQueryDTO dto) {
        String queueName = dto.getQueueName();
        String sql = "select * from "+TABLE_NAME+" where 1=1 ";
        String countSql = "SELECT COUNT(*) FROM "+TABLE_NAME + " where 1=1 ";
        
        if (dto.getQueueName()!=null){
            sql = sql + " and  `queue` ="+"'"+queueName+"'";
            countSql = countSql + " and  `queue` ="+"'"+queueName+"'";
        }
        
        Long offset = dto.getOffset();
        if (offset!=null){
            sql = sql + " and  `offset` >"+offset;
            countSql = countSql + " and  `offset` >"+offset;
        }
        Long lastOffset = dto.getLastOffset();
        if (lastOffset!=null){
            sql = sql + " and  `offset` <="+lastOffset;
            countSql = countSql + " and  `offset` >"+offset;
        }
        sql = sql +" ORDER BY `offset` desc";
        
        Long pageIndex = dto.getPage();
        Long pageSize = dto.getSize();
        long begin = (pageIndex-1) * pageSize;
        long end = pageIndex * pageSize;
        sql = sql + " limit "+begin+","+end;
        
        
       
        int totalElements = jdbcTemplate.queryForObject(countSql, Integer.class);
        PageResult<HistoryMessageVO> messageVOPageResult = new PageResult<>();
        if (totalElements<=0){
            return messageVOPageResult;
        }
        messageVOPageResult.setTotal(totalElements);
        log.info("pageMessageList sql:{}",sql);
        List<Map<String, Object>> query = jdbcTemplate.queryForList(sql);
        if (CollectionUtils.isEmpty(query)){
             return messageVOPageResult;
        }
        List<HistoryMessageVO> messageList=new ArrayList<>();
        for (Map<String, Object> map : query) {
            HistoryMessageVO message = new HistoryMessageVO();
            message.setQueue((String)map.get("queue"));
            message.setBody(map.get("body"));
            message.setId((String)map.get("id"));
            message.setTag((String)map.get("tag"));
            message.setKey((String)map.get("key"));
            message.setVirtualQueueName((String)map.get("virtual_queue_name"));
            message.setOffset(Long.parseLong(map.get("offset").toString()));
            message.setExecuteTime(Long.parseLong(map.get("execute_time").toString()));
            message.setExecuteScope( Long.parseLong(map.get("executor_scope").toString()));
            message.setCreateTime(map.get("create_time").toString());
            message.setUpdateTime(map.get("update_time").toString());
            message.setHeader(StringUtils.isNotBlank(map.get("header").toString()) ?RedisMQStringMapper.toMap(map.get("header").toString()):null);
            message.setStatus(Integer.parseInt(map.get("status").toString()));
            messageList.add(message);
        }
        messageVOPageResult.setList(messageList);
        return messageVOPageResult;
    }
}
