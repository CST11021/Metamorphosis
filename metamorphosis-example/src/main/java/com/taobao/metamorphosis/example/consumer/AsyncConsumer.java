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
 * 异步消息消费者
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class AsyncConsumer {
    public static void main(final String[] args) throws Exception {
        // 1、初始化客户端配置
        MetaClientConfig config = initMetaConfig();

        // 2、创建消息会话工厂
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

        // 3、创建消费者
        final String group = "meta-example";
        ConsumerConfig consumerConfig = new ConsumerConfig(group);
        // 默认最大获取延迟为5秒，这里设置成100毫秒，请根据实际应用要求做设置，测试的时候如果使用默认值5秒，会有消费延迟的现象
        consumerConfig.setMaxDelayFetchTimeInMills(100);
        final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);

        // 4、订阅消息，并消费消息
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