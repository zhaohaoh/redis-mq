package com.redismq.samples.rocket;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;


@Slf4j
public class FileLogUtil {

    public static Message getMessage(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                     final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case -626843481:
                    break;
            }

            byte[] bytesContent = new byte[totalSize];
            // 3 bodyCRC
            int bodyCRC = byteBuffer.getInt();
            // 4 QUEUEID

            int queueLen = byteBuffer.getInt();
            byte[] queue = new byte[queueLen];
            byteBuffer.get(queue);
            System.out.println(new String(queue));

            // 5 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            // 6 BODY
            int bodyLen = byteBuffer.getInt();
            String body = null;
            if (bodyLen > 0) {
                if (readBody) {
                  byteBuffer.get(bytesContent, 0, bodyLen);
                    body=new String(bytesContent,0,bodyLen,StandardCharsets.UTF_8);
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

            // 7 topic
            int topicLen = byteBuffer.getInt();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, StandardCharsets.UTF_8);

            // 7 tag
            int tagLen = byteBuffer.getInt();
            byte[] tag = new byte[tagLen];
            byteBuffer.get(tag);
            String tagStr = new String(tag);

            Message message = new Message();
            message.setTopic(topic);
            message.setVirtualQueueName(new String(queue));
            message.setBody(body);
            message.setTag(tagStr);
            System.out.println(message);
            return message;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }
}
