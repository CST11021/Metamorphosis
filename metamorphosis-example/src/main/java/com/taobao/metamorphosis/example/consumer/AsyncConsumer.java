/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.example.consumer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * �첽��Ϣ������
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class AsyncConsumer {
    public static void main(final String[] args) throws Exception {
        // 1����ʼ���ͻ�������
        MetaClientConfig config = initMetaConfig();

        // 2��������Ϣ�Ự����
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

        // 3������������
        final String group = "meta-example";
        ConsumerConfig consumerConfig = new ConsumerConfig(group);
        // Ĭ������ȡ�ӳ�Ϊ5�룬�������ó�100���룬�����ʵ��Ӧ��Ҫ�������ã����Ե�ʱ�����ʹ��Ĭ��ֵ5�룬���������ӳٵ�����
        consumerConfig.setMaxDelayFetchTimeInMills(100);
        final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);

        // 4��������Ϣ����������Ϣ
        final String topic = "meta-test";
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
            }

            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }

        });
        consumer.completeSubscribe();
    }

}