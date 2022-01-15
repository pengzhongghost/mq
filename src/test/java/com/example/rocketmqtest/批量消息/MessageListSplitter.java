package com.example.rocketmqtest.批量消息;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

//todo 消息列表分割器只会处理每条消息的大小不超过4m的情况，
//     若存在某条消息其本身大小大于4m，此分割器无法处理，
//     其直接将这条消息构成一个子列表返回
public class MessageListSplitter implements Iterator<List<Message>> {

    //指定极限值
    public final int SIZE_LIMIT = 4 * 1024 * 1024;
    //存放所有要发送的消息
    public final List<Message> messages;
    //要进行批量发送消息的小集合起始索引
    private int currentIndex;

    public MessageListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currentIndex;
        //记录当前要发送的这一小批次的大小
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            //生产者发送的消息大小为 topic的长度+body的长度+20字节的消息日志的长度+properties中键值对的长度
            int tempSize = message.getBody().length + message.getTopic().length();
            for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
                tempSize += entry.getKey().length() + entry.getValue().length();
            }
            tempSize += 20;
            //判断当前消息本身是否是大于4m
            if (tempSize > SIZE_LIMIT) {
                if (nextIndex - currentIndex == 0) {
                    //下面sublist一个
                    nextIndex++;
                }
                break;
            }
            if (tempSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                totalSize += tempSize;
            }
        }
        List<Message> subList = this.messages.subList(currentIndex, nextIndex);
        //记录下次开始遍历的位置
        currentIndex = nextIndex;
        return subList;
    }

}
