package com.redismq.samples;

import com.redismq.samples.controller.ProducerController;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = SamplesApplication.class)
@Slf4j
@RunWith(SpringRunner.class)
public class Test1 {
    @Autowired
    private ProducerController producerController;


    @Test
    public void sendMail() {
        producerController.sendMessage();
    }
}
