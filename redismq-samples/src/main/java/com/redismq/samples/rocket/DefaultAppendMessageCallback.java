package com.redismq.samples.rocket;

import com.redismq.utils.RedisMQObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class DefaultAppendMessageCallback {
    // File at the end of the minimum fixed length empty
    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    private final ByteBuffer msgIdMemory;
    private final ByteBuffer msgIdV6Memory;
    // Store the message content
    private final ByteBuffer msgStoreItemMemory;
    // The maximum length of the message
    private final int maxMessageSize;
    // Build Message Key
    private final StringBuilder keyBuilder = new StringBuilder();

    private final StringBuilder msgIdBuilder = new StringBuilder();

    DefaultAppendMessageCallback(final int size) {
        this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
        this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
        this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
        this.maxMessageSize = size;
    }

    public ByteBuffer getMsgStoreItemMemory() {
        return msgStoreItemMemory;
    }

    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final Message msgInner) {
        // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

        // PHY OFFSET
        long wroteOffset = fileFromOffset + byteBuffer.position();

        String s = RedisMQObjectMapper.toJsonStr(msgInner.getBody());
        byte[] body = s.getBytes(StandardCharsets.UTF_8);

        byte[] queue = msgInner.getVirtualQueueName().getBytes();
        final int bodyLength = msgInner.getBody() == null ? 0 : body.length;
        byte[] topic = msgInner.getTopic().getBytes();

        byte[] tag = msgInner.getTag().getBytes();
        //  totalLength=4 MAGICCODE=4   BODYCRC=4  PHYSICALOFFSET=8  bodyLength=4  queueLen=4   topicLen=4 tagLen=4
        int totalLength = bodyLength + queue.length + topic.length +tag.length +  4 + 4 + 4 +4  +4 +4+ 8 +4;
        // Initialization of storage space
        //切换位置到0，并且可以读写的位置是消息的长度。这个缓存区只是临时存储内存消息用的。所以切换到0可以初始化空间
        this.msgStoreItemMemory.flip();
        this.msgStoreItemMemory.limit(totalLength);
        // 1 TOTALSIZE
        this.msgStoreItemMemory.putInt(totalLength);
        // 2 MAGICCODE
        this.msgStoreItemMemory.putInt(-626843481);
        // 3 BODYCRC
        this.msgStoreItemMemory.putInt(CrcUtil.crc32(body));
        // 4 QUEUEID
        this.msgStoreItemMemory.putInt(queue.length);
        this.msgStoreItemMemory.put(queue);
        // 7 PHYSICALOFFSET
        long value = fileFromOffset + byteBuffer.position();
        this.msgStoreItemMemory.putLong(value);
        // 15 BODY
        this.msgStoreItemMemory.putInt(bodyLength);
        if (bodyLength > 0)
            this.msgStoreItemMemory.put(body);
        // 16 TOPIC
        this.msgStoreItemMemory.putInt(topic.length);
        this.msgStoreItemMemory.put(topic);
        // TAG
        this.msgStoreItemMemory.putInt(tag.length);
        this.msgStoreItemMemory.put(tag);

        // Write messages to the queue buffer
        byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgStoreItemMemory.limit());

        AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
        appendMessageResult.setTotalSize(totalLength);
        return appendMessageResult;
    }


}