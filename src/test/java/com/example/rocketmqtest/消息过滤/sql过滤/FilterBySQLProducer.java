package com.example.rocketmqtest.消息过滤.sql过滤;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class FilterBySQLProducer {
    @SneakyThrows
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("filterTagGroup");
        producer.setNamesrvAddr("192.168.227.129:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] body = ("hi," + i).getBytes();
            Message message = new Message("filterSQLTopic", "myTag", body);
            //事先埋入用户属性
            message.putUserProperty("age", i + "");
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
