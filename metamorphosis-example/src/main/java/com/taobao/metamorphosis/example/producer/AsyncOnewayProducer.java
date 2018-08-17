package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.AsyncMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.Help;


/**
 * <pre>
 * 异步单向消息发送者 *
 * 
 * 
 * 用于创建异步单向发送消息的会话工厂.
 * 
 * 使用场景:
 *      对于发送可靠性要求不那么高,但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性.
 *      例如,收集日志或用户行为信息等场景.
 * 注意:
 *      发送消息后返回的结果中不包含准确的messageId,partition,offset,这些值都是-1
 * 
 * @author 无花
 * @Date 2012-2-27
 * 
 */
public class AsyncOnewayProducer {
    public static void main(final String[] args) throws Exception {
        final String topic = "meta-test";
        final AsyncMessageSessionFactory sessionFactory = new AsyncMetaMessageSessionFactory(initMetaConfig());
        final MessageProducer producer = sessionFactory.createAsyncProducer();
        producer.publish(topic);
        Help.sendMessage(producer, topic, new SendMessageCallback() {

            @Override
            public void onMessageSent(final SendResult result) {
                if (result.isSuccess()) {
                    System.out.println("Send message successfully,sent to " + result.getPartition());
                }
                else {
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
