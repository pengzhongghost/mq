package com.example.rocketmqtest.延迟消息;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;


public class DelayMessage {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.227.129:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] body = ("hi," + i).getBytes();
            Message message = new Message("delayTopic", "delayTag", body);
            //设置消息延迟的等级，即延迟10s
            message.setDelayTimeLevel(3);
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        producer.shutdown();
    }
}
