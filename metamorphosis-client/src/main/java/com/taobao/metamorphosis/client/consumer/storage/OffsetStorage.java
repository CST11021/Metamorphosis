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
package com.taobao.metamorphosis.client.consumer.storage;

import java.util.Collection;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * Offset存储器接口
 *
 * Offset存储说明：
 *
 * MetaQ的消费模型是一种拉取的模型，消费者根据上次消费数据的绝对偏移量(offset)从服务端的数据文件中拉取后面的数据继续消费，因此这个offset
 * 信息就非常关键，需要可靠地保存。默认情况下，MetaQ是将offset信息保存在你使用的zookeeper集群上，也就是ZkOffsetStorage所做的事情，它实
 * 现了OffsetStorage接口。通常这样的保存是可靠并且安全的，但是有时候可能你也需要其他选项，目前还提供两个不同的OffsetStorage实现：
 *
 * 1、LocalOffsetStorage，使用consumer的本地文件作为offset存储，默认存储在${HOME}/.meta_offsets的文件里。适合消费者分组只有一个消费者的
 * 情况，无需共享offset信息。例如广播类型的消费者就特别合适。
 *
 * 2、MysqlOffsetStorage，使用Mysql作为offset存储，使用前需要创建表结构：
 *
 * CREATE TABLE `meta_topic_partition_group_offset` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `topic` varchar(255) NOT NULL,
 *   `partition` varchar(255) NOT NULL,
 *   `group_id` varchar(255) NOT NULL,
 *   `offset` int(11) NOT NULL,
 *   `msg_id` int(11) NOT NULL,
 *   PRIMARY KEY (`id`),
 *   KEY `TOPIC_PART_GRP_IDX` (`topic`,`partition`,`group_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * 你也可以实现自己的OffsetStorage存储。如果你想使用除了zookeeper之外的offset存储，可以在创建消费者的时候传入：
 *
 *   consumer  sessionFactorycreateConsumer(consumerConfig, (dataSource));
 * mysql存储需要传入JDBC数据源。
 *
 *
 *
 * 第一次消费的offset初始值。前面提到ConsumerConfig有个offset参数可以设置第一次消费的时候开始的绝对偏移量，默认这个参数是0，也就是从服务
 * 端现有消息的最小偏移量开始，从头开始消费所有消息。但是，通常情况下，新的消费分组都是希望从最新的消息开始消费，ComsumerConfig提供了一个
 * setConsumeFromMaxOffset(boolean always)方法来设置从最新位置开始消费。其中always参数表示是否每次消费者启动都从最新位置开始消费，
 * 这样就忽略了在消费者停止期间的消息。通常仅在测试的时候将always参数设置为true，以便每次测试最新的消息。除非你真的不需要消费者停止期间（
 * 比如重启间隔）的消息，否则不要将always设置为true。
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
public interface OffsetStorage {

    /**
     * 初始化offset
     *
     * @param topic
     * @param group
     * @param partition
     * @param offset
     */
    public void initOffset(String topic, String group, Partition partition, long offset);

    /**
     * 保存offset到存储
     * 
     * @param group     消费者组名
     * @param infoList  消费者订阅的消息分区信息列表
     */
    public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList);

    /**
     * 加载一条消费者的订阅信息，如果不存在返回null
     * 
     * @param topic
     * @param group
     * @param partition
     * @return
     */
    public TopicPartitionRegInfo load(String topic, String group, Partition partition);

    /**
     * 释放资源，meta客户端在关闭的时候会主动调用此方法
     */
    public void close();

}