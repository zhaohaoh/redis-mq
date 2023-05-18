/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * $Id: MappedFileTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package com.redismq.samples.rocket;

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
        Message message=new Message();
        message.setBody("测试数据");
        message.setTopic("aa");
        message.setVirtualQueueName("fffff");
        message.setId("111");

        AppendMessageResult appendMessageResult = mappedFile.appendMessagesInner(message, new DefaultAppendMessageCallback(1024 * 64));
        System.out.println(appendMessageResult);
        ByteBuffer slice = mappedFile.getMappedByteBuffer().slice();

        MessageCommitLog commitLog = FileLogUtil.checkMessageAndReturnSize(slice, true, true);
        System.out.println(commitLog);
        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectCurrentMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        System.out.println((readString).equals(storeMessage));

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
