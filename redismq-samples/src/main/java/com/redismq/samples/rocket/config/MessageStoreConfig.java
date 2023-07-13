//package com.redismq.samples.rocket.config;
//
//import com.redismq.constant.FlushDiskType;
//import lombok.Data;
//
//import java.io.File;
//
//@Data
//public class MessageStoreConfig {
//    //The root directory in which the log data is kept
//    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
//
//    //The directory in which the commitlog is kept
//    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
//        + File.separator + "commitlog";
//
//    // CommitLog file size,default is 1G
//    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
//
//    // CommitLog flush interval
//    // flush data to disk  刷盘间隔时间
//    private int flushIntervalCommitLog = 500;
//
//    /**
//     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
//     * By default it is set to false indicating using spin lock when putting message.
//     */
//    private boolean useReentrantLockWhenPutMessage = false;
//
//    // Whether schedule flush,default is real-time
//    private boolean flushCommitLogTimed = false;
//    // ConsumeQueue flush interval  刷新ConsumeQueue的间隔
//    private int flushIntervalConsumeQueue = 1000;
//    // Resource reclaim interval  不管
//    private int cleanResourceInterval = 10000;
//    // CommitLog removal interval  删除文件的时间间隔，每次删除要等一等
//    private int deleteCommitLogFilesInterval = 100;
//    // ConsumeQueue removal interval
//    private int deleteConsumeQueueFilesInterval = 100;
//    // 不懂先备注 文件删除第一次拒绝后，所能保留的最大间隔时间，默认1000 * 120，单位毫秒
//    private int destroyMapedFileIntervalForcibly = 1000 * 120;
//    //删除的定时任务执行的间隔
//    private int redeleteHangedFileInterval = 1000 * 120;
//    // When to delete,default is at 4 am  删除的时间点，默认是凌晨4点
//    private String deleteWhen = "04";
//    //检测物理文件磁盘空间  默认75%  超过则报警
//    private int diskMaxUsedSpaceRatio = 75;
//    // The number of hours to keep a log file before deleting it (in hours)
//    // 文件保留时间，
//    private int fileReservedTime = 72;
//    // Flow control for ConsumeQueue
//    private int putMsgIndexHightWater = 600000;
//    // The maximum size of message,default is 4M
//    private int maxMessageSize = 1024 * 1024 * 4;
//    // Whether check the CRC32 of the records consumed.
//    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
//    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
//    private boolean checkCRCOnRecover = true;
//    // How many pages are to be flushed when flush CommitLog  异步刷新时 刷盘的页数
//    private int flushCommitLogLeastPages = 4;
//    // How many pages are to be committed when commit data to file
//    private int commitCommitLogLeastPages = 4;
//    // Flush page size when the disk in warming state 如果开启预热warmMapedFileEnable 预热文件大小
//    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
//    // How many pages are to be flushed when flush ConsumeQueue  刷盘的页数
//    private int flushConsumeQueueLeastPages = 2;
//    // 异步刷盘的参数  异步刷盘的频率，间隔
//    private int flushCommitLogThoroughInterval = 1000 * 10;
//    //获取配置的 提交commitLog最大频次  默认200ms
//    private int commitCommitLogThoroughInterval = 200;
//    //consumequeue频率
//    private int flushConsumeQueueThoroughInterval = 1000 * 60;
//    @ImportantField
//    //消息在内存中的最大值
//    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
//    @ImportantField
//    private int maxTransferCountOnMessageInMemory = 32;
//    @ImportantField
//    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
//    @ImportantField
//    private int maxTransferCountOnMessageInDisk = 8;
//    @ImportantField
//    private int accessMessageInMemoryMaxRatio = 40;
//    @ImportantField
//    private boolean messageIndexEnable = true;
//    private int maxHashSlotNum = 5000000;
//    private int maxIndexNum = 5000000 * 4;
//    private int maxMsgsNumBatch = 64;
//    @ImportantField
//    private boolean messageIndexSafe = false;
//    private int haListenPort = 10912;
//    private int haSendHeartbeatInterval = 1000 * 5;
//    private int haHousekeepingInterval = 1000 * 20;
//    private int haTransferBatchSize = 1024 * 32;
//    @ImportantField
//    private String haMasterAddress = null;
//    private int haSlaveFallbehindMax = 1024 * 1024 * 256;
//    @ImportantField
//    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
//    @ImportantField
//    //    刷盘策略
//    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
//    //同步刷盘超时时间
//    private int syncFlushTimeout = 1000 * 5;
//    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
//
//
//    private long flushDelayOffsetInterval = 1000 * 10;
//    @ImportantField
//    private boolean cleanFileForciblyEnable = true;
//    private boolean warmMapedFileEnable = false;
//    private boolean offsetCheckInSlave = false;
//    private boolean debugLockEnable = false;
//    private boolean duplicationEnable = false;
//    private boolean diskFallRecorded = true;
//    private long osPageCacheBusyTimeOutMills = 1000;
//    //默认最大队列数量
//    private int defaultQueryMaxNum = 32;
//
//    @ImportantField
//    private boolean transientStorePoolEnable = false;
//    private int transientStorePoolSize = 5;
//    private boolean fastFailIfNoBufferInStorePool = false;
//
//    private boolean enableDLegerCommitLog = false;
//    private String dLegerGroup;
//    private String dLegerPeers;
//    private String dLegerSelfId;
//
//
//}
