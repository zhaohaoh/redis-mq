package com.redismq.samples.rocket;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;


@Slf4j
public class FileLogUtil {

    public static MessageCommitLog  checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                      final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case -626843481:
                    break;
//                case BLANK_MAGIC_CODE:
//                    return new DispatchRequest(0, true /* success */);
//                default:
//                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
//                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            long physicOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = CrcUtil.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
//                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, StandardCharsets.UTF_8);

            MessageCommitLog commitLog = new MessageCommitLog();
            commitLog.setTopic(topic);
            commitLog.setTotalSize(totalSize);
            commitLog.setTopic(topic);
            return commitLog;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }
}
