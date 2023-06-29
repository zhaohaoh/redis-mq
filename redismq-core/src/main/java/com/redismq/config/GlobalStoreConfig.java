package com.redismq.config;

import com.redismq.constant.FlushDiskType;
import lombok.Data;

import java.io.File;

@Data
public class GlobalStoreConfig {
    /**
     * 单条消息最大值
     */
    private int maxMessageSize = 1024 * 4;
    /**
     * 存储根路径
     */
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    /**
     * 存储commitlog的路径
     */
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";

    /**
     * commitlog默认大小
     */
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    /**
     * 刷盘策略
     */
    private FlushDiskType flushDiskType = FlushDiskType.SYNC_FLUSH;
    /**
     * 内存预热
     */
    private boolean isWarmMapedFileEnable=false;
    /**
     * 刷盘超时时间
     */
    private int flushTimeOut = 500;
}
