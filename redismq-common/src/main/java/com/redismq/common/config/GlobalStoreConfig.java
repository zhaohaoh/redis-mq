package com.redismq.common.config;

import com.redismq.common.constant.StoreEnum;
import lombok.Data;

import java.time.Duration;

@Data
public class GlobalStoreConfig {
    /**
     * 存储消息的策略
     */
    private StoreEnum storeType = StoreEnum.MYSQL;
    
    /**
     * 过期时间
     */
    private Duration expireTime = Duration.ofDays(30);
//    /**
//     * 单条消息最大值
//     */
//    private int maxMessageSize = 1024 * 4;
//    /**
//     * 存储根路径
//     */
//    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
//
//    /**
//     * 存储commitlog的路径
//     */
//    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
//            + File.separator + "commitlog";
//
//    /**
//     * commitlog默认大小
//     */
//    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
//
//    /**
//     * 刷盘策略
//     */
//    private FlushDiskType flushDiskType = FlushDiskType.SYNC_FLUSH;
//    /**
//     * 内存预热
//     */
//    private boolean isWarmMapedFileEnable=false;
//    /**
//     * 刷盘超时时间
//     */
//    private int flushTimeOut = 500;
//
//    /**
//     * 定时刷盘时间间隔
//     */
//    private int flushInterval = 500;
//
//    /**
//     *  是否定时刷盘
//     */
//    private boolean isScheduleTimeFlush=true;
//
//    /**
//     *  至少提交日志页面
//     */
//    private int flushCommitLogLeastPages=4;
//
//    /**
//     *  提交日志彻底间隔
//     */
//    private int flushCommitLogThoroughInterval = 1000 * 10;
}
