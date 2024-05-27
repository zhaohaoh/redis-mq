//
//package com.redismq.samples;
//
//import com.redismq.samples.rocket.store.MappedFile;
//import com.redismq.samples.rocket.SelectMappedBufferResult;
//import org.junit.After;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//
//public class MappedFileTest {
//    private final String storeMessage = "Once, there was a chance for me!";
//
//    @Test
//    public void testSelectMappedBuffer() throws IOException {
//        MappedFile mappedFile = new MappedFile("target/unit_test_store/MappedFileTest/000", 1024 * 64);
//        boolean result = mappedFile.appendMessage(storeMessage.getBytes());
//        System.out.println(result);
//
//        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
//        byte[] data = new byte[storeMessage.length()];
//        selectMappedBufferResult.getByteBuffer().get(data);
//        String readString = new String(data);
//
//        System.out.println((readString).equals(storeMessage));
//
//        mappedFile.shutdown(1000);
//
//        System.out.println(mappedFile.isAvailable());
//        selectMappedBufferResult.release();
//        System.out.println(mappedFile.isCleanupOver());
//
//    }
//
//    @After
//    public void destory() {
//        File file = new File("target/unit_test_store");
////        UtilAll.deleteFile(file);
//    }
//}
