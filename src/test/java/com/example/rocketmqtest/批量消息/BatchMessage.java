package com.example.rocketmqtest.批量消息;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

public class BatchMessage {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batchGroup");
        producer.setNamesrvAddr("192.168.227.129:9876");
        //指定要发送消息的最大大小 默认是4m
        // 此处改没用，还需要该配置文件
        producer.setMaxMessageSize(4 * 1024 * 1024);
        producer.start();
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] body = ("hi," + i).getBytes();
            Message message = new Message("batchTopic", "batchTag", body);
            messages.add(message);
        }
        //定义消息列表分割器，将消息列表分割为不超过4m大小的小列表
        MessageListSplitter messageListSplitter = new MessageListSplitter(messages);
        while (messageListSplitter.hasNext()) {
            List<Message> messageList = messageListSplitter.next();
            SendResult sendResult = producer.send(messageList);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }
}
