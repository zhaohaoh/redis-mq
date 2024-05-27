//package com.redismq.samples.rocket.store;
//
//import com.redismq.samples.rocket.ReferenceResource;
//import com.redismq.samples.rocket.SelectMappedBufferResult;
//import com.redismq.samples.util.UtilAll;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.util.CollectionUtils;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.ListIterator;
//import java.util.concurrent.CopyOnWriteArrayList;
//
///**
// * 映射文件队列
// *
// * @author hzh
// * @date 2023/06/20
// */
//@Slf4j
//public class MappedFileQueue {
//    private static final int DELETE_FILES_BATCH_MAX = 10;
//
//    private final String storePath;
//
//    private final int mappedFileSize;
//
//    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
//
//    private long flushedWhere = 0;
//    private long committedWhere = 0;
//    private volatile long storeTimestamp = 0;
//
//    public long getFlushedWhere() {
//        return flushedWhere;
//    }
//
//    public void setFlushedWhere(long flushedWhere) {
//        this.flushedWhere = flushedWhere;
//    }
//
//    public long getStoreTimestamp() {
//        return storeTimestamp;
//    }
//
//    public List<MappedFile> getMappedFiles() {
//        return mappedFiles;
//    }
//
//    public int getMappedFileSize() {
//        return mappedFileSize;
//    }
//
//    public long getCommittedWhere() {
//        return committedWhere;
//    }
//
//    public void setCommittedWhere(final long committedWhere) {
//        this.committedWhere = committedWhere;
//    }
//
//    /**
//     * 映射文件队列
//     *
//     * @param storePath      存储路径
//     * @param mappedFileSize 映射文件大小
//     */
//    public MappedFileQueue(final String storePath, int mappedFileSize) {
//        this.storePath = storePath;
//        this.mappedFileSize = mappedFileSize;
//    }
//
//    /**
//     * 获取最新映射文件根据时间
//     *
//     * @param timestamp 时间戳
//     * @return {@link MappedFile}
//     */
//    public MappedFile getMappedFileByTime(final long timestamp) {
//        if (CollectionUtils.isEmpty(mappedFiles)) {
//            return null;
//        }
//        Object[] mfs = mappedFiles.toArray();
//
//        for (Object mf : mfs) {
//            MappedFile mappedFile = (MappedFile) mf;
//            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
//                return mappedFile;
//            }
//        }
//
//        return (MappedFile) mfs[mfs.length - 1];
//    }
//
//
//    /**
//     * 截断肮脏文件
//     *
//     * @param offset 当前读取到的偏移量偏移量
//     */
//    public void truncateDirtyFiles(long offset) {
//        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
//
//        for (MappedFile file : this.mappedFiles) {
//            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
//            //文件的最后偏移量大于读取到的offset。说明文件没写满。如果是写满的文件这两个值应该是一样的。没写满是最新的文件
//            if (fileTailOffset > offset) {
//                // 读取偏移量大于文件名字当前位置。设置偏移量当前偏移量。为什么取模。因为这读取偏移量的值是多个文件的总和。但是mappedFileSize是一个文件的大小
//                if (offset >= file.getFileFromOffset()) {
//                    //文件偏移量取模进行设置
//                    file.setWrotePosition((int) (offset % this.mappedFileSize));
//                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
//                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
//                } else {
//                    //文件当前位置超过了读取到的当前偏移量，此文件直接删除。属于多余的数据 因为rocketmq的文件名字是偏移量走的。超过偏移量的文件就是有问题
//                    //整个文件删除
//                    file.destroy(1000);
//                    willRemoveFiles.add(file);
//                }
//            }
//        }
//        //删除文件。简单，不解析
//        this.deleteFile(willRemoveFiles);
//    }
//
//    /**
//     * 删除文件
//     *
//     * @param files 文件
//     */
//    public void deleteFile(List<MappedFile> files) {
//        if (!files.isEmpty()) {
//            //取交集
//            files.retainAll(mappedFiles);
//            try {
//                if (!this.mappedFiles.removeAll(files)) {
//                    log.error("deleteExpiredFile remove failed.");
//                }
//            } catch (Exception e) {
//                log.error("deleteExpiredFile has exception.", e);
//            }
//        }
//    }
//
//    /**
//     * 加载文件到本地映射。初始化阶段执行
//     */
//    public boolean load() {
//        File dir = new File(this.storePath);
//        File[] files = dir.listFiles();
//        if (files != null) {
//            // ascending order
//            Arrays.sort(files);
//            for (File file : files) {
//
//                if (file.length() != this.mappedFileSize) {
//                    log.warn(file + "\t" + file.length()
//                            + " length not matched message store config value, please check it manually");
//                    return false;
//                }
//
//                try {
//                    // 初始化设置映射文件的大小。以mappedFileSize 1024M。后续会重新加载
//                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
//                    mappedFile.setWrotePosition(this.mappedFileSize);
//                    mappedFile.setFlushedPosition(this.mappedFileSize);
//                    mappedFile.setCommittedPosition(this.mappedFileSize);
//                    this.mappedFiles.add(mappedFile);
//                    log.info("load " + file.getPath() + " OK");
//                } catch (IOException e) {
//                    log.error("load file " + file + " error", e);
//                    return false;
//                }
//            }
//        }
//        return true;
//    }
//
//    /**
//     * 获取最后一个映射文件
//     *
//     * @param startOffset 起始偏移量
//     * @param needCreate  需要创建
//     * @return {@link MappedFile}
//     */
//    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
//        long createOffset = -1;
//        MappedFile mappedFileLast = getLastMappedFile();
//
//        if (mappedFileLast == null) {
//            createOffset = startOffset - (startOffset % this.mappedFileSize);
//        }
//
//        if (mappedFileLast != null && mappedFileLast.isFull()) {
//            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
//        }
//
//        if (createOffset != -1 && needCreate) {
//            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
//            MappedFile mappedFile = null;
//            //创建一个新文件
//            try {
//                mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
//            } catch (IOException e) {
//                log.error("create mappedFile exception", e);
//            }
//
//            if (mappedFile != null) {
//                if (this.mappedFiles.isEmpty()) {
//                    mappedFile.setFirstCreateInQueue(true);
//                }
//                this.mappedFiles.add(mappedFile);
//            }
//
//            return mappedFile;
//        }
//
//        return mappedFileLast;
//    }
//
//    public MappedFile getLastMappedFile(final long startOffset) {
//        return getLastMappedFile(startOffset, true);
//    }
//
//    /**
//     * 获取最后一个映射文件
//     *
//     * @return {@link MappedFile}
//     */
//    public MappedFile getLastMappedFile() {
//        MappedFile mappedFileLast = null;
//
//        while (!this.mappedFiles.isEmpty()) {
//            try {
//                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
//                break;
//            } catch (IndexOutOfBoundsException e) {
//                //continue;
//            } catch (Exception e) {
//                log.error("getLastMappedFile has exception.", e);
//                break;
//            }
//        }
//
//        return mappedFileLast;
//    }
//
//    /**
//     * 重置偏移量
//     */
//    public boolean resetOffset(long offset) {
//        MappedFile mappedFileLast = getLastMappedFile();
//
//        if (mappedFileLast != null) {
//            long lastOffset = mappedFileLast.getFileFromOffset() +
//                    mappedFileLast.getWrotePosition();
//            long diff = lastOffset - offset;
//            //不超过两个文件的大小
//            final int maxDiff = this.mappedFileSize * 2;
//            if (diff > maxDiff)
//                return false;
//        }
//
//        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
//        // 从队列后往前遍历，如果当前mappedFile的起始指针大于offset，则需要丢弃该文件。
//        // 否则第一次遇到mappedFile的起始指针不大于offset，就将当前文件的三个指针设置为offset，然后跳出循环
//        // 即丢弃offset以后的所有数据
//        while (iterator.hasPrevious()) {
//            mappedFileLast = iterator.previous();
//            if (offset >= mappedFileLast.getFileFromOffset()) {
//                int where = (int) (offset % mappedFileLast.getFileSize());
//                mappedFileLast.setFlushedPosition(where);
//                mappedFileLast.setWrotePosition(where);
//                mappedFileLast.setCommittedPosition(where);
//                break;
//            } else {
//                iterator.remove();
//            }
//        }
//        return true;
//    }
//
//    /**
//     * 获取整个队列的（第一个文件）最小偏移量
//     */
//    public long getMinOffset() {
//
//        if (!this.mappedFiles.isEmpty()) {
//            try {
//                return this.mappedFiles.get(0).getFileFromOffset();
//            } catch (IndexOutOfBoundsException e) {
//                //continue;
//            } catch (Exception e) {
//                log.error("getMinOffset has exception.", e);
//            }
//        }
//        return -1;
//    }
//
//    /**
//     * 获取整个队列（最后一个文件）的最大偏移量
//     */
//    public long getMaxOffset() {
//        MappedFile mappedFile = getLastMappedFile();
//        if (mappedFile != null) {
//            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
//        }
//        return 0;
//    }
//
//    /**
//     * 获取最大的写位置
//     */
//    public long getMaxWrotePosition() {
//        MappedFile mappedFile = getLastMappedFile();
//        if (mappedFile != null) {
//            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
//        }
//        return 0;
//    }
//
//    public long remainHowManyDataToCommit() {
//        return getMaxWrotePosition() - committedWhere;
//    }
//
//    public long remainHowManyDataToFlush() {
//        return getMaxOffset() - flushedWhere;
//    }
//
//    /**
//     * 删除最后一个映射文件
//     */
//    public void deleteLastMappedFile() {
//        MappedFile lastMappedFile = getLastMappedFile();
//        if (lastMappedFile != null) {
//            lastMappedFile.destroy(1000);
//            this.mappedFiles.remove(lastMappedFile);
//            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
//
//        }
//    }
//
//    /**
//     * 删除过期文件根据时间
//     *
//     * @param expiredTime         过期时间
//     * @param deleteFilesInterval 删除文件时间间隔
//     * @param intervalForcibly    间隔强行
//     * @param cleanImmediately    立即清洁
//     * @return int
//     */
//    public int deleteExpiredFileByTime(final long expiredTime,
//                                       final int deleteFilesInterval,
//                                       final long intervalForcibly,
//                                       final boolean cleanImmediately) {
//        Object[] mfs = this.copyMappedFiles(0);
//
//        if (null == mfs)
//            return 0;
//
//        int mfsLength = mfs.length - 1;
//        int deleteCount = 0;
//        List<MappedFile> files = new ArrayList<MappedFile>();
//        if (null != mfs) {
//            for (int i = 0; i < mfsLength; i++) {
//                MappedFile mappedFile = (MappedFile) mfs[i];
//                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
//                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
//                    if (mappedFile.destroy(intervalForcibly)) {
//                        files.add(mappedFile);
//                        deleteCount++;
//
//                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
//                            break;
//                        }
//
//                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
//                            try {
//                                Thread.sleep(deleteFilesInterval);
//                            } catch (InterruptedException e) {
//                            }
//                        }
//                    } else {
//                        break;
//                    }
//                } else {
//                    //avoid deleting files in the middle
//                    break;
//                }
//            }
//        }
//
//        deleteFile(files);
//
//        return deleteCount;
//    }
//
//    /**
//     * 删除过期文件根据偏移量  ConsumeQueue使用的
//     */
//    public int deleteExpiredFileByOffset(long offset, int unitSize) {
//        Object[] mfs = this.copyMappedFiles(0);
//
//        List<MappedFile> files = new ArrayList<MappedFile>();
//        int deleteCount = 0;
//        if (null != mfs) {
//
//            int mfsLength = mfs.length - 1;
//
//            for (int i = 0; i < mfsLength; i++) {
//                boolean destroy;
//                MappedFile mappedFile = (MappedFile) mfs[i];
//                //表示获取当前mappedFile文件最后一个消息
//                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
//                if (result != null) {
//                    //接着读取最后一个消息的前8个字节得到消息的物理偏移量
//                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
//                    result.release();
//                    destroy = maxOffsetInLogicQueue < offset;
//                    if (destroy) {
//                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
//                                + maxOffsetInLogicQueue + ", delete it");
//                    }
//                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
//                    log.warn("Found a hanged consume queue file, attempting to delete it.");
//                    destroy = true;
//                } else {
//                    log.warn("this being not executed forever.");
//                    break;
//                }
//
//                if (destroy && mappedFile.destroy(1000 * 60)) {
//                    files.add(mappedFile);
//                    deleteCount++;
//                } else {
//                    break;
//                }
//            }
//        }
//
//        deleteFile(files);
//
//        return deleteCount;
//    }
//
//    /**
//     * 刷盘 是从页缓存到磁盘
//     */
//    public boolean flush(final int flushLeastPages) {
//        boolean result = true;
//        // 根据当前刷盘的偏移量获取当前的文件
//        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
//        if (mappedFile != null) {
//            //刷盘，并且重新写刷盘位置
//            long tmpTimeStamp = mappedFile.getStoreTimestamp();
//            int offset = mappedFile.flush(flushLeastPages);
//            long where = mappedFile.getFileFromOffset() + offset;
//            result = where == this.flushedWhere;
//            this.flushedWhere = where;
//            if (0 == flushLeastPages) {
//                this.storeTimestamp = tmpTimeStamp;
//            }
//        }
//
//        return result;
//    }
//
//    /**
//     * Finds a mapped file by offset.
//     *
//     * @param offset                Offset.
//     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
//     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
//     */
//    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
//        try {
//            MappedFile firstMappedFile = this.getFirstMappedFile();
//            MappedFile lastMappedFile = this.getLastMappedFile();
//            if (firstMappedFile != null && lastMappedFile != null) {
//                // offset比第一个文件小，或者offset比最后一个文件大。说明偏移量有问题呀
//                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
//                    log.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
//                            offset,
//                            firstMappedFile.getFileFromOffset(),
//                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
//                            this.mappedFileSize,
//                            this.mappedFiles.size());
//                } else {
//                    //判断偏移量在哪个文件
//                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
//                    MappedFile targetFile = null;
//                    try {
//                        targetFile = this.mappedFiles.get(index);
//                    } catch (Exception ignored) {
//                    }
//
//                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
//                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
//                        return targetFile;
//                    }
//
//                    for (MappedFile tmpMappedFile : this.mappedFiles) {
//                        if (offset >= tmpMappedFile.getFileFromOffset()
//                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
//                            return tmpMappedFile;
//                        }
//                    }
//                }
//
//                if (returnFirstOnNotFound) {
//                    return firstMappedFile;
//                }
//            }
//        } catch (Exception e) {
//            log.error("findMappedFileByOffset Exception", e);
//        }
//
//        return null;
//    }
//
//    public MappedFile getFirstMappedFile() {
//        MappedFile mappedFileFirst = null;
//
//        if (!this.mappedFiles.isEmpty()) {
//            try {
//                mappedFileFirst = this.mappedFiles.get(0);
//            } catch (IndexOutOfBoundsException e) {
//                //ignore
//            } catch (Exception e) {
//                log.error("getFirstMappedFile has exception.", e);
//            }
//        }
//
//        return mappedFileFirst;
//    }
//
//    public MappedFile findMappedFileByOffset(final long offset) {
//        return findMappedFileByOffset(offset, false);
//    }
//
//    /**
//     * 获取有效的文件的大小总和
//     */
//    public long getMappedMemorySize() {
//        long size = 0;
//
//        Object[] mfs = this.copyMappedFiles(0);
//        if (mfs != null) {
//            for (Object mf : mfs) {
//                if (((ReferenceResource) mf).isAvailable()) {
//                    size += this.mappedFileSize;
//                }
//            }
//        }
//
//        return size;
//    }
//
//    public void shutdown(final long intervalForcibly) {
//        for (MappedFile mf : this.mappedFiles) {
//            mf.shutdown(intervalForcibly);
//        }
//    }
//
//    public void destroy() {
//        for (MappedFile mf : this.mappedFiles) {
//            mf.destroy(1000 * 3);
//        }
//        this.mappedFiles.clear();
//        this.flushedWhere = 0;
//
//        // delete parent directory
//        File file = new File(storePath);
//        if (file.isDirectory()) {
//            file.delete();
//        }
//    }
//
//    private Object[] copyMappedFiles(final int reservedMappedFiles) {
//        Object[] mfs;
//
//        if (this.mappedFiles.size() <= reservedMappedFiles) {
//            return null;
//        }
//
//        mfs = this.mappedFiles.toArray();
//        return mfs;
//    }
//}
