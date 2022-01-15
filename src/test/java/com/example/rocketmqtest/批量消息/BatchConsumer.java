package com.example.rocketmqtest.批量消息;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchConsumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batchConsumer");
        consumer.setNamesrvAddr("192.168.227.129:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("batchTopic", "*");
        //指定每次可以消费10条消息，默认为1
        consumer.setConsumeMessageBatchMaxSize(10);
        //指定每次可以从broker拉取40条消息，默认32
        consumer.setPullBatchSize(40);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            /**
             * @SneakyThrow 将避免 javac 坚持要您捕获或向前抛出您方法主体中的语句声明它们生成的任何已检查异常。
             * @SneakyThrow 不会默默吞下、包装到 RuntimeException 或以其他方式修改列出的已检查异常类型的任何异常。
             * JVM 不检查受检异常系统的一致性； javac 确实如此，并且此注释使您可以选择退出其机制。完整的文档可以在 @SneakyThrows 的项目 lombok 功能页面中找到。
             */
            @SneakyThrows
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                }
                TimeUnit.SECONDS.sleep(3);
                //消费成功的返回结果
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        TimeUnit.SECONDS.sleep(10);
    }
}
