package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.AsyncMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.AsyncMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.Help;


/**
 * <pre>
 * �첽������Ϣ������ *
 * 
 * 
 * ���ڴ����첽��������Ϣ�ĻỰ����.
 * 
 * ʹ�ó���:
 *      ���ڷ��Ϳɿ���Ҫ����ô��,��Ҫ����߷���Ч�ʺͽ��Ͷ�����Ӧ�õ�Ӱ�죬�������Ӧ�õ��ȶ���.
 *      ����,�ռ���־���û���Ϊ��Ϣ�ȳ���.
 * ע��:
 *      ������Ϣ�󷵻صĽ���в�����׼ȷ��messageId,partition,offset,��Щֵ����-1
 * 
 * @author �޻�
 * @Date 2012-2-27
 * 
 */
public class AsyncOnewayProducer {
    public static void main(final String[] args) throws Exception {

        // 1����ʼ���ͻ�������
        MetaClientConfig config = initMetaConfig();

        // 2��������Ϣ�Ự������һ��Ự������ʹ�õ���������
        final AsyncMetaMessageSessionFactory sessionFactory = new AsyncMetaMessageSessionFactory(config);

        // 3������������
        final MessageProducer producer = sessionFactory.createAsyncProducer();

        // 4������topic
        final String topic = "meta-test";
        producer.publish(topic);

        // 5��������Ϣ
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
