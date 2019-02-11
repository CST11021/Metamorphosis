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
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.example.Help;


/**
 * ͬ������Ϣ�����ߣ�ͬ������Ϣ���������첽����Ϣ�����ߵ��������ڷ�����Ϣʱ�Ƿ�ʹ�ûص��ӿڣ�
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class SyncProducer {

    public static void main(final String[] args) throws Exception {

        // 1����ʼ���ͻ�������
        MetaClientConfig config = initMetaConfig();

        // 2��������Ϣ�Ự������һ��Ự������ʹ�õ���������
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

        // 3��������Ϣ������
        final MessageProducer producer = sessionFactory.createProducer();

        // 4������topic,��topicע�ᵽzk
        final String topic = "meta-test";
        producer.publish(topic);

        // 5��������Ϣ
        Help.sendMessage(producer, topic);
    }
}