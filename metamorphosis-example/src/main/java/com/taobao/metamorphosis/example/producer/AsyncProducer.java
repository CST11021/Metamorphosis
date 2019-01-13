package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.Help;


/**
 * 异步消息发送者
 * 
 * @author 无花
 * @Date 2012-2-27
 * 
 */
public class AsyncProducer {

    public static void main(final String[] args) throws Exception {

        // 1、初始化客户端配置
        MetaClientConfig config = initMetaConfig();

        // 2、创建消息会话工厂：一般会话工厂会使用单例来创建
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

        // 3、创建消息生产者
        final MessageProducer producer = sessionFactory.createProducer();

        // 4、发布topic
        final String topic = "slave-test";
        producer.publish(topic);

        // 5、发送消息
        Help.sendMessage(producer, topic, new SendMessageCallback() {

            @Override
            public void onMessageSent(final SendResult result) {
                if (result.isSuccess()) {
                    System.out.println("Send message successfully,sent to " + result.getPartition());
                } else {
                    System.err.println("Send message failed,error message:" + result.getErrorMessage());
                }
            }

            @Override
            public void onException(final Throwable e) {
                e.printStackTrace();
            }
        });
    }
}
