package com.redismq.samples.controller;

import com.redismq.common.connection.RedisClient;
import com.redismq.common.pojo.Message;
import com.redismq.samples.consumer.JavaBean;
import com.redismq.utils.RedisMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("producer")
public class ProducerController {
    @Autowired
    private RedisMQTemplate redisMQTemplate;

    @Autowired
    private RedisClient redisClient;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @PostMapping("sendDelayMessage")
    public void sendDelayMessage() {
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 10000; i++) {
            executorService.submit(() -> {
                Map<String, Object> header = new HashMap<>();
                header.put("duplicateId", "id");
                Message message = Message.builder()
                        .body(buildJavaBean())
                        .queue("delaytest1")
                        .header(header)
                        .build();
                redisMQTemplate.sendDelayMessage(message, 0L, TimeUnit.MICROSECONDS);
            });
        }
        executorService.shutdown();
    }

    @PostMapping("sendDelayMessage2")
    public void sendDelayMessage2() {
        redisMQTemplate.sendTimingMessage(
                buildJavaBean(),
                "delaytest1",
                System.currentTimeMillis() + Duration.ofSeconds(1).toMillis()
        );
    }

    public Map<String, Object> sendMessage() {
        return sendMessage(10, 10, 10);
    }

    @PostMapping("sendMessage")
    public Map<String, Object> sendMessage(@RequestParam(required = false) Integer batchSize,
                                           @RequestParam(required = false) Integer batchTimes,
                                           @RequestParam(required = false) Integer concurrency) {
        int safeBatchSize = normalizePositive(batchSize, 10, "batchSize");
        int safeBatchTimes = normalizePositive(batchTimes, 10, "batchTimes");
        int safeConcurrency = normalizePositive(concurrency, safeBatchTimes, "concurrency");
        int threadCount = Math.min(safeConcurrency, safeBatchTimes);
        long startTime = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>(safeBatchTimes);
        try {
            for (int i = 0; i < safeBatchTimes; i++) {
                futures.add(executorService.submit(() -> sendNormalMessageBatch(safeBatchSize)));
            }
            int sentCount = 0;
            for (Future<Integer> future : futures) {
                sentCount += future.get();
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("queue", "test1");
            result.put("batchSize", safeBatchSize);
            result.put("batchTimes", safeBatchTimes);
            result.put("concurrency", threadCount);
            result.put("totalMessages", sentCount);
            result.put("durationMs", System.currentTimeMillis() - startTime);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("send normal message interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("send normal message failed", e.getCause());
        } finally {
            executorService.shutdown();
        }
    }

    @PostMapping("sendOrderMessage")
    public void sendOrderMessage() {
        redisMQTemplate.sendMessage("\u987a\u5e8f\u6d88\u606f\u6d88\u8d39", "order");
    }

    @PostMapping("sendTimingMessage")
    public void sendTimingMessage() {
        LocalDateTime time = LocalDateTime.of(2023, 12, 14, 14, 20, 30);
        long timestamp = time.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        redisMQTemplate.sendTimingMessage(buildJavaBean(), "time", "bussiness1", timestamp);
    }

    @PostMapping("sendMultiTagMessage")
    public void sendMultiTagMessage() {
        for (int i = 0; i < 100; i++) {
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d391", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d392", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d391", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d392", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d391", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d392", "MultiTag", "bussiness2");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d391", "MultiTag", "bussiness1");
            redisMQTemplate.sendMessage("\u591a\u4e2a\u6807\u7b7e\u540c\u4e00Queue\u6d88\u606f\u6d88\u8d392", "MultiTag", "bussiness2");
        }
    }

    private int sendNormalMessageBatch(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            redisMQTemplate.sendMessage(buildJavaBean(), "test1");
        }
        return batchSize;
    }

    private int normalizePositive(Integer value, int defaultValue, String fieldName) {
        int actualValue = value == null ? defaultValue : value;
        if (actualValue <= 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than 0");
        }
        return actualValue;
    }

    private JavaBean buildJavaBean() {
        JavaBean javaBean = new JavaBean();
        javaBean.setA("ff");
        javaBean.setB(222);
        return javaBean;
    }
}
