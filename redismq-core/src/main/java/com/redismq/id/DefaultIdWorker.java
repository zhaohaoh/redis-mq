package com.redismq.id;

import com.redismq.config.GlobalConfigCache;

import java.util.concurrent.ThreadLocalRandom;

/**
 * <p>名称：IdWorker.java</p>
 * <p>描述：分布式自增长ID</p>
 * <pre>
 *     Twitter的 Snowflake　JAVA实现方案
 * </pre>
 * 核心代码为其IdWorker这个类实现，其原理结构如下，我分别用一个0表示一位，用—分割开部分的作用：
 * 1||0---0000000000 0000000000 0000000000 0000000000 0 --- 00000 ---00000 ---000000000000
 * 在上面的字符串中，第一位为未使用（实际上也可作为long的符号位），接下来的41位为毫秒级时间，
 * 然后5位datacenter标识位，5位机器ID（并不算标识符，实际是为线程标识），
 * 然后12位该毫秒内的当前毫秒内的计数，加起来刚好64位，为一个Long型。
 * 这样的好处是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞（由datacenter和机器ID作区分），
 * 并且效率较高，经测试，snowflake每秒能够产生26万ID左右，完全满足需要。
 * <p>
 * 64位ID (42(毫秒)+5(机器ID)+5(业务编码)+12(重复累加))
 *
 * @author Polim
 * <p>
 * 共计机器32台X32 版本
 * 标准版
 */
public class DefaultIdWorker {
    // 时间起始标记点，作为基准，一般取系统的最近时间（一旦确定不能变动）
    private final long twepoch = 1713953445000L;
    // 机器标识位数    意思就是最多代表 2 ^ 5 个机房（32 个机房）  从0开始标识位
    private final long workerIdBits = GlobalConfigCache.GLOBAL_CONFIG.maxWorkerIdBits;
    // 机器ID最大值 2的3次方-1  7    机器id 0-7
    private final long maxWorkerId = ~(-1L << workerIdBits);
    
    
    // 毫秒内自增位
    private final long sequenceBits = 10L;
    // 机器ID偏左移12位
    private final long workerIdShift = sequenceBits;
    
    // 时间毫秒左移22位
    private final long timestampLeftShift = sequenceBits + workerIdBits ;
    
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);
    /* 上次生产id时间戳 */
    private long lastTimestamp = -1L;
    // 0，并发控制
    private long sequence = 0L;
    
    private final long workerId;
    
    
    /**
     * @param workerId     工作机器ID
     */
    public DefaultIdWorker(long workerId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        this.workerId = workerId;
    }
    
    /**
     * 获取下一个ID
     *
     */
    public synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= 10) {
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        throw new RuntimeException(String.format("雪花算法时钟回滚 距离当前时间还差%d milliseconds", offset));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(String.format("雪花算法时钟回滚 距离当前时间还差%d milliseconds", offset));
            }
        }
        
        if (lastTimestamp == timestamp) {
            // 当前毫秒内，则+1
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 不同毫秒内，序列号置为 1 - 3 随机数
            sequence = ThreadLocalRandom.current().nextLong(1, 3);
        }
        
        lastTimestamp = timestamp;
        
        // ID偏移组合生成最终的ID，并返回ID
        return ((timestamp - twepoch) << timestampLeftShift)   | (workerId << workerIdShift) | sequence;
    }
    
    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }
    
    private long timeGen() {
        return System.currentTimeMillis();
    }

    
}