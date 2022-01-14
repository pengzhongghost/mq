package com.example.rocketmqtest;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RocketmqTestApplicationTests {

    @Test
    void contextLoads() {
    }

    /**
     * todo 同步发送消息
     */
    @Test
    void syncProducer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //创建一个producer，参数为producer group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameserver名称
        producer.setNamesrvAddr("192.168.227.129:9876");
        //设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时间为5s，默认是3是
        producer.setSendMsgTimeout(5000);
        //开启生产者
        producer.start();
        //发送消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes(StandardCharsets.UTF_8);
            Message message = new Message("someTopic", "someTag", body);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();
    }

    /**
     * todo 异步发送消息
     */
    @Test
    void asyncProducer() throws Exception {
        //创建一个producer，参数为producer group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameserver名称
        producer.setNamesrvAddr("192.168.227.129:9876");
        //设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时间为5s，默认是3是
        producer.setSendMsgTimeout(5000);
        //开启生产者
        producer.start();
        //发送消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes(StandardCharsets.UTF_8);
            Message message = new Message("myTopic", "myTag", body);
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        }
        //异步发送的
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }

    /**
     * 单向发送
     */
    @Test
    void onewayProducer() throws Exception {
        //创建一个producer，参数为producer group名称
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        //指定nameserver名称
        producer.setNamesrvAddr("192.168.227.129:9876");
        //设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        //设置发送超时时间为5s，默认是3是
        producer.setSendMsgTimeout(5000);
        //开启生产者
        producer.start();
        //发送消息
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes(StandardCharsets.UTF_8);
            Message message = new Message("single", "single", body);
            /**
             * 与 UDP 类似，此方法在返回之前不会等待代理的确认。显然，它具有最大的吞吐量，但也有可能丢失消息。
             * 参数： msg - 要发送的消息。抛出：MQClientException – 如果有任何客户端错误。
             * RemotingException – 如果有任何网络层错误。 InterruptedException – 如果发送线程被中断。
             */
            producer.sendOneway(message);
        }
        producer.shutdown();
    }

    /**
     * 消费消息
     */
    @Test
    void consumer() throws Exception {
        //consumerGroup必须要指定，否则会报错
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        consumer.setNamesrvAddr("192.168.227.129:9876");
        //指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //指定消费的topic和tag
        consumer.subscribe("someTopic", "*");
        //指定采用广播模式进行消费，默认为集群模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //注册一个消息监听
        //一旦broker中有了其订阅的消息就会触发该方法的执行
        //返回值为当前consumer消费的状态
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    //System.out.println(msg);
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启消费
        consumer.start();
        TimeUnit.SECONDS.sleep(5);
        System.out.println("consumer start");
    }

}
