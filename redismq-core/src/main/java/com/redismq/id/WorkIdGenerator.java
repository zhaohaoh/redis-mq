package com.redismq.id;

import com.redismq.common.constant.RedisMQConstant;
import com.redismq.common.connection.RedisClient;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class WorkIdGenerator {
    
    private final int maxWorkId ;
    
    private static final String WORK_ID_ZSET = RedisMQConstant.getWorkIdZset();
    
    private RedisClient redisClient;
    
    public WorkIdGenerator(RedisClient redisClient,Integer workerIdBits) {
        this.redisClient = redisClient;
        this.maxWorkId =  ~(-1 << workerIdBits);;
    }
    
    public Integer getSnowId() {
        Boolean success = redisClient.setIfAbsent(WORK_ID_ZSET+ "_LOCK", "", Duration.ofSeconds(5));
        while (success==null || !success) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
            success = redisClient.setIfAbsent(WORK_ID_ZSET+ "_LOCK", "", Duration.ofSeconds(5));
        }
        try {
            Map<Integer, Double> mapDoubleMap = redisClient.zRangeWithScores(WORK_ID_ZSET, 0, 0, Integer.class);
            if (CollectionUtils.isEmpty(mapDoubleMap)) {
                init();
            }
            mapDoubleMap = redisClient.zRangeWithScores(WORK_ID_ZSET, 0, 0, Integer.class);
            Optional<Integer> first = mapDoubleMap.keySet().stream().findFirst();
            Integer workId = first.get();
            redisClient.zAdd(WORK_ID_ZSET, workId, System.currentTimeMillis());
            return workId;
        }finally {
            redisClient.delete(WORK_ID_ZSET+ "_LOCK");
        }
    }
    
    public void init() {
        for (int i = 0; i < maxWorkId; i++) {
            redisClient.zAdd(WORK_ID_ZSET, i, System.currentTimeMillis());
        }
    }
}
