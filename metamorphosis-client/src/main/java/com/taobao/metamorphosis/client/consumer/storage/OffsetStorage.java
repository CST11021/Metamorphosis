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
 * Offset�洢���ӿ�
 *
 * Offset�洢˵����
 *
 * MetaQ������ģ����һ����ȡ��ģ�ͣ������߸����ϴ��������ݵľ���ƫ����(offset)�ӷ���˵������ļ�����ȡ��������ݼ������ѣ�������offset
 * ��Ϣ�ͷǳ��ؼ�����Ҫ�ɿ��ر��档Ĭ������£�MetaQ�ǽ�offset��Ϣ��������ʹ�õ�zookeeper��Ⱥ�ϣ�Ҳ����ZkOffsetStorage���������飬��ʵ
 * ����OffsetStorage�ӿڡ�ͨ�������ı����ǿɿ����Ұ�ȫ�ģ�������ʱ�������Ҳ��Ҫ����ѡ�Ŀǰ���ṩ������ͬ��OffsetStorageʵ�֣�
 *
 * 1��LocalOffsetStorage��ʹ��consumer�ı����ļ���Ϊoffset�洢��Ĭ�ϴ洢��${HOME}/.meta_offsets���ļ���ʺ������߷���ֻ��һ�������ߵ�
 * ��������蹲��offset��Ϣ������㲥���͵������߾��ر���ʡ�
 *
 * 2��MysqlOffsetStorage��ʹ��Mysql��Ϊoffset�洢��ʹ��ǰ��Ҫ������ṹ��
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
 * ��Ҳ����ʵ���Լ���OffsetStorage�洢���������ʹ�ó���zookeeper֮���offset�洢�������ڴ��������ߵ�ʱ���룺
 *
 *   consumer  sessionFactorycreateConsumer(consumerConfig, (dataSource));
 * mysql�洢��Ҫ����JDBC����Դ��
 *
 *
 *
 * ��һ�����ѵ�offset��ʼֵ��ǰ���ᵽConsumerConfig�и�offset�����������õ�һ�����ѵ�ʱ��ʼ�ľ���ƫ������Ĭ�����������0��Ҳ���Ǵӷ���
 * ��������Ϣ����Сƫ������ʼ����ͷ��ʼ����������Ϣ�����ǣ�ͨ������£��µ����ѷ��鶼��ϣ�������µ���Ϣ��ʼ���ѣ�ComsumerConfig�ṩ��һ��
 * setConsumeFromMaxOffset(boolean always)���������ô�����λ�ÿ�ʼ���ѡ�����always������ʾ�Ƿ�ÿ��������������������λ�ÿ�ʼ���ѣ�
 * �����ͺ�������������ֹͣ�ڼ����Ϣ��ͨ�����ڲ��Ե�ʱ��always��������Ϊtrue���Ա�ÿ�β������µ���Ϣ����������Ĳ���Ҫ������ֹͣ�ڼ䣨
 * �����������������Ϣ������Ҫ��always����Ϊtrue��
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
public interface OffsetStorage {

    /**
     * ��ʼ��offset
     *
     * @param topic
     * @param group
     * @param partition
     * @param offset
     */
    public void initOffset(String topic, String group, Partition partition, long offset);

    /**
     * ����offset���洢
     * 
     * @param group     ����������
     * @param infoList  �����߶��ĵ���Ϣ������Ϣ�б�
     */
    public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList);

    /**
     * ����һ�������ߵĶ�����Ϣ����������ڷ���null
     * 
     * @param topic
     * @param group
     * @param partition
     * @return
     */
    public TopicPartitionRegInfo load(String topic, String group, Partition partition);

    /**
     * �ͷ���Դ��meta�ͻ����ڹرյ�ʱ����������ô˷���
     */
    public void close();

}