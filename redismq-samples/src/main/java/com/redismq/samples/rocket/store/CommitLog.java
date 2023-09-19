
package com.redismq.samples.rocket.store;

import com.redismq.config.GlobalConfigCache;
import com.redismq.constant.FlushDiskType;
import com.redismq.samples.rocket.*;
import com.redismq.samples.rocket.config.PutMessageResult;
import com.redismq.samples.rocket.config.PutMessageStatus;
import com.redismq.samples.rocket.lock.PutMessageLock;
import com.redismq.samples.rocket.lock.PutMessageSpinLock;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Store all metadata downtime for recovery, data protection reliability
 */
@Slf4j
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    // End of file empty MAGIC CODE cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;
    protected final MappedFileQueue mappedFileQueue;
    private final ServiceThread serviceThread;

    private final DefaultAppendMessageCallback appendMessageCallback;
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;

    protected final PutMessageLock putMessageLock;
    private final int mappedFileSize;

    public CommitLog() {
        int mappedFileSizeCommitLog = GlobalConfigCache.GLOBAL_STORE_CONFIG.getMappedFileSizeCommitLog();
        this.mappedFileQueue = new MappedFileQueue(GlobalConfigCache.GLOBAL_STORE_CONFIG.getStorePathCommitLog(),
                mappedFileSizeCommitLog);

        this.mappedFileSize = mappedFileSizeCommitLog;

        if (FlushDiskType.SYNC_FLUSH == GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushDiskType()) {
            this.serviceThread = new SyncService();
        } else {
            this.serviceThread = new AsyncService();
        }

        this.appendMessageCallback = new DefaultAppendMessageCallback(GlobalConfigCache.GLOBAL_STORE_CONFIG.getMaxMessageSize());

        this.putMessageLock = new PutMessageSpinLock();
    }

    public boolean load() {
        //把所有的文件加载到文件内存队列中
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * 开始文件任务
     */
    public void start() {
        this.serviceThread.start();
    }

    /**
     * 关闭文件任务
     */
    public void shutdown() {
        this.serviceThread.shutdown();
        this.mappedFileQueue.destroy();
    }

    /**
     *  立即刷盘 0 代表不指定到达页数才刷盘 即立即刷
     *
     */
    public long flush() {
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 获取整个队列最后一个偏移量，也就是最新的偏移量
     *
     * @return long
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    /**
     * 当前还有多少数据没刷盘
     *
     * @return long
     */
    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    /**
     * 删除过期文件
     *
     * @param expiredTime         过期时间
     * @param deleteFilesInterval 删除文件时间间隔
     * @param intervalForcibly    间隔强行
     * @param cleanImmediately    立即清洁
     */
    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 获取数据
     *
     * @param offset                抵消
     * @param returnFirstOnNotFound 返回第一个未找到
     * @return {@link SelectMappedBufferResult}
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     * 正常退出流程
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        //校验crc
        boolean checkCRCOnRecover = true;
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            //从最后第三个文件开始恢复
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //这个值不是0开始的，而是文件的名字的偏移量。所以下面是从文件的名字偏移量开始，不断自增消息的大小，就能得到这个文件目前的偏移量
            long processOffset = mappedFile.getFileFromOffset();
            //临时文件偏移
            long mappedFileOffset = 0;
            while (true) {
                //循环读取文件的
                int size = FileLogUtil.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, true);
                // Normal data  成功并且有消息。当前临时文件偏移量增加
                if (size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                //说明当前读完了，要读下一个文件
                else {
                    index++;
                    if (index >= mappedFiles.size()) {
                        //完全读完了  这是正常读完的流程
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        //设置下一个文件开始读
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //清理超出的文件 同时设置mappedFile偏移量
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
        }
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    /**
     * 重置偏移量
     */
    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * 异步推送消息
     *
     * @param msg 味精
     * @return {@link CompletableFuture}<{@link PutMessageResult}>
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final Message msg) {

        // Back to Results
        AppendMessageResult result = null;

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = System.currentTimeMillis();
            this.beginTimeInLock = System.currentTimeMillis();

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file2 error, topic: " + msg.getTopic());
                {
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
                }
            }
            result = mappedFile.appendMessagesInner(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessagesInner(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody(), result);
        }

        if (null != unlockMappedFile && GlobalConfigCache.GLOBAL_STORE_CONFIG.isWarmMapedFileEnable()) {
            // this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result);
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
        return flushResultFuture.thenApply((flushStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }
            return putMessageResult;
        });

    }


    public PutMessageResult putMessage(final Message msg) {

        // Back to Results
        AppendMessageResult result = null;

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = System.currentTimeMillis();
            this.beginTimeInLock = System.currentTimeMillis();

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file2 error, topic: " + msg.getTopic());
                {
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
                }
            }
            result = mappedFile.appendMessagesInner(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessagesInner(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            elapsedTimeInLock = System.currentTimeMillis() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody(), result);
        }

        if (null != unlockMappedFile && GlobalConfigCache.GLOBAL_STORE_CONFIG.isWarmMapedFileEnable()) {
            // this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
        handleDiskFlush(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * 提交刷新请求
     *
     * @param result 结果
     * @return {@link CompletableFuture}<{@link PutMessageStatus}>
     */
    public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result) {
        // Synchronization flush
        if (FlushDiskType.SYNC_FLUSH == GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushDiskType()) {
            final SyncService service = (SyncService) this.serviceThread;
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getTotalSize(),
                    GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushTimeOut());
            service.putRequest(request);
            return request.future();
        }
        return null;
    }

    /**
     * 处理磁盘刷新
     *
     * @param result           结果
     * @param putMessageResult 把消息结果
     * @param message          消息
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, Message message) {
        // Synchronization flush
        if (FlushDiskType.SYNC_FLUSH == GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushDiskType()) {
            final SyncService service = (SyncService) this.serviceThread;

            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getTotalSize());
            service.putRequest(request);
            CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
            PutMessageStatus flushStatus = null;
            try {
                flushStatus = flushOkFuture.get(GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushTimeOut(),
                        TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                //flushOK=false;
            }
            if (flushStatus != PutMessageStatus.PUT_OK) {
                log.error("do groupcommit, wait for flush failed, topic: " + message.getTopic() + " tags: " + message.getTag());
                putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }
        }
    }


    /**
     * 获取最小值偏移量
     *
     * @return long
     */
    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     *  根据偏移量和长度获取消息
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 滚动到下一个文件的位置
     *
     */
    public long rollNextFile(final long offset) {
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    /**
     *  直接写入文件
     */
    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    /**
     * 异步刷新服务
     *
     * @author hzh
     * @date 2023/06/29
     */
    class AsyncService extends ServiceThread {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;
        private final int interval = GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushInterval();
        private int flushPhysicQueueLeastPages = GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushCommitLogLeastPages();
        private final int flushPhysicQueueThoroughInterval = GlobalConfigCache.GLOBAL_STORE_CONFIG.getFlushCommitLogThoroughInterval();
        private final boolean scheduleTimeFlush = GlobalConfigCache.GLOBAL_STORE_CONFIG.isScheduleTimeFlush();

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                //如果一次处理超过了时间的间隔，说明刷盘速度落后了,那么flushPhysicQueueLeastPages=0 即立刻刷盘
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    log.error("async flush  busy");
                }

                try {
                    //定时刷盘或唤醒刷盘
                    if (scheduleTimeFlush) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
//                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
//                    if (storeTimestamp > 0) {
//                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
//                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < 10 && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }


            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return AsyncService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long startTimestamp = System.currentTimeMillis();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final boolean flushOK) {
            long endTimestamp = System.currentTimeMillis();
            PutMessageStatus result = (flushOK && ((endTimestamp - this.startTimestamp) <= this.timeoutMillis)) ?
                    PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT;
            this.flushOKFuture.complete(result);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * GroupCommit Service
     */
    class SyncService extends ServiceThread {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();
        private final PutMessageLock putMessageLock = new PutMessageSpinLock();

        public void putRequest(final GroupCommitRequest request) {
            putMessageLock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                putMessageLock.unlock();
            }

            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        private void swapRequests() {
            putMessageLock.lock();
            try {
                List<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                putMessageLock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = false;
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        //说明已经刷盘了
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                        if (!flushOK) {
                            CommitLog.this.mappedFileQueue.flush(0);
                        }
                    }

                    req.wakeupCustomer(flushOK);
                }
                //后面要重点关注 这段代码宕机恢复机制
//                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
//                    if (storeTimestamp > 0) {
//                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
//                    }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.warn("GroupCommitService Exception, ", e);
            }


            this.swapRequests();

            this.doCommit();

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return SyncService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }
}
