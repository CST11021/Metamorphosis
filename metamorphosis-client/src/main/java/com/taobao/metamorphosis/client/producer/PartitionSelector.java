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
package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 分区选择器,生产者发送消息时，使用该分区选择器选择一个分区保存消息，默认使用循环的方式选择分区
 *
 * 生产者在通过zk获取分区列表之后，会按照brokerId和分区号的顺序排列组织成一个有序的分区列表，发送的时候按照从头到尾循环往复的方式选择一个
 * 分区来发送消息。这是默认的分区策略，考虑到我们的broker服务器软硬件配置基本一致，默认的轮询策略已然足够。如果你想实现自己的负载均衡策略，
 * 可以自己实现PartitionSelector接口，并在创建producer的时候传入即可。
 *
 * 在broker因为重启或者故障等因素无法服务的时候，producer通过zookeeper会感知到这个变化，将失效的分区从列表中移除做到fail over。
 * 因为从故障到感知变化有一个延迟，可能在那一瞬间会有部分的消息发送失败。
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public interface PartitionSelector {

    /**
     * 根据topic、message从partitions列表中选择分区
     * 
     * @param topic         表示该消息所属的主题
     * @param partitions    表示可选择的分区列表
     * @param message       消息对象
     *
     * @return
     * @throws MetaClientException 此方法抛出的任何异常都应当包装为MetaClientException
     */
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
}