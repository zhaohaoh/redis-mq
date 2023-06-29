package com.redismq.samples.rocket;

import com.redismq.samples.rocket.store.DefaultAppendMessageCallback;
import com.redismq.samples.rocket.store.MappedFile;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MappedFileTest {
    private final String storeMessage = "Once, there was a chance for me!";

    @Test
    public void testSelectMappedBuffer() throws IOException {
        MappedFile mappedFile = new MappedFile("target/unit_test_store/MappedFileTest/000", 1024 * 64);
        Message message = new Message();
        message.setBody("测试数据");
        message.setTopic("topic");
        message.setVirtualQueueName("fffff");
        message.setId("111");
        message.setTag("tag");
        AppendMessageResult appendMessageResult = mappedFile.appendMessagesInner(message, new DefaultAppendMessageCallback(1024 * 64));

        System.out.println(appendMessageResult);
        // slice重新创建一个新的缓冲区
        ByteBuffer slice = mappedFile.getMappedByteBuffer().slice();
        long offset = 0;
        while (true) {
          int returnSize = FileLogUtil.checkMessageAndReturnSize(slice, true, true);
          if (returnSize <=0 ){
              break;
          }
            offset+=returnSize;
        }
        mappedFile.setWrotePosition((int) (offset % (1024 * 64)));
        mappedFile.setFlushedPosition((int) (offset % (1024 * 64)));


        mappedFile.appendMessagesInner(message, new DefaultAppendMessageCallback(1024 * 64));
        ByteBuffer aaa = mappedFile.getMappedByteBuffer().slice();

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);


        mappedFile.shutdown(1000);
        System.out.println(mappedFile.isAvailable());
        selectMappedBufferResult.release();
        System.out.println(mappedFile.isCleanupOver());
    }

    @After
    public void destory() {
        File file = new File("target/unit_test_store");
//        UtilAll.deleteFile(file);
    }
}
