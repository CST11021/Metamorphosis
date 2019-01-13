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
import java.util.List;
import java.util.Properties;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.OrderedMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.OrderedMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.producer.OrderedMessagePartitionSelector;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.example.Help;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 严格顺序发送消息
 * 
 * @author 无花
 * @since 2012-2-22 下午4:28:11
 */

public class OrderedProducer {
    public static void main(final String[] args) throws Exception {

        // 1、初始化客户端配置
        final MetaClientConfig metaClientConfig = initMetaConfig();
        // 设置分区分布情况,要跟服务端对应
        final Properties partitionsInfo = new Properties();
        partitionsInfo.put("topic.num.exampleTopic1", "0:4;1:4");
        metaClientConfig.setPartitionsInfo(partitionsInfo);

        // 2、创建消息会话工厂：一般会话工厂会使用单例来创建
        final OrderedMessageSessionFactory sessionFactory = new OrderedMetaMessageSessionFactory(metaClientConfig);

        // 3、创建生产者，这里使用自定义的分区选择器
        final MessageProducer producer = sessionFactory.createProducer(new CustomPartitionSelector());

        // 4、发布topic
        final String topic = "meta-test";
        producer.publish(topic);

        // 5、发送消息
        Help.sendMessage(producer, topic);
    }



    /**
     * 自定义的分区选择器
     */
    static class CustomPartitionSelector extends OrderedMessagePartitionSelector {

        @Override
        protected Partition choosePartition(final String topic, final List<Partition> partitions, final Message message) {
            // 根据一定的规则把需要有序的局部消息路由到同一个分区
            final int hashCode = new String(message.getData()).hashCode();
            final int partitionNo = hashCode % partitions.size();
            return partitions.get(partitionNo);
        }
    }
}