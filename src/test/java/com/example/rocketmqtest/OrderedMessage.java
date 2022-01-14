package com.example.rocketmqtest;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 顺序消息
 */
@SpringBootTest
public class OrderedMessage {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("orderedGroup");
        producer.setNamesrvAddr("192.168.227.129:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            int orderId = i;
            Message message = new Message("orderedTopic", "orderedTag", ("hi" + orderId).getBytes(StandardCharsets.UTF_8));
            //设置消息key传过去
            message.setKeys(orderId + "");
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    //使用消息key作为选择key
                    String keys = msg.getKeys();
                    Integer id = Integer.valueOf(keys);
                    /**使用arg作为选择key的选择算法
                     * //arg指的是send方法第三个参数传的orderId
                     Integer id = (Integer) arg;
                     */
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }

}
