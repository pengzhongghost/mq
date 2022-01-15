package com.example.rocketmqtest.消息过滤.tag过滤;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class FilterByTagProducer {

    @SneakyThrows
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("filterTagGroup");
        producer.setNamesrvAddr("192.168.227.129:9876");
        producer.start();
        String[] tags = {"tagA", "tagB", "tagC"};
        for (int i = 0; i < 10; i++) {
            byte[] body = ("hi," + i).getBytes();
            String tag = tags[i % tags.length];
            Message message = new Message("filterTagTopic",tag,body);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }

}
