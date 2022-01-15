package com.example.rocketmqtest.事务消息;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionMessage {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("tpg");
        producer.setNamesrvAddr("192.168.227.129:9876");
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        //为生产者指定一个线程池
        producer.setExecutorService(executorService);
        //添加事务监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             * todo 当发送事务准备（半）消息成功时，将调用该方法执行本地事务。
             *      参数：msg – Half(prepare) message arg – 自定义业务参数 返回：事务状态
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("预提交消息成功：" + msg);
                if (StringUtils.equals("TAGA", msg.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TAGB", msg.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if (StringUtils.equals("TAGC", msg.getTags())) {
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            //消息回查方法
            /**
             * todo 当没有响应准备（一半）消息时。 broker 会发送 check 消息检查交易状态，
             *      调用该方法获取本地交易状态。参数：msg - 检查消息返回：事务状态
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("执行消息回查" + msg.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        String[] tags = {"TAGA", "TAGB", "TAGC"};
        for (int i = 0; i < 3; i++) {
            byte[] body = ("hi," + i).getBytes();
            Message message = new Message("transactionTopic", tags[i], body);
            //发送事务消息
            //第二个参数用于指定在执行本地事务时要使用的业务参数 TransactionListener.executeLocalTransaction
            TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, null);
            System.out.println(transactionSendResult);
        }

    }
}
