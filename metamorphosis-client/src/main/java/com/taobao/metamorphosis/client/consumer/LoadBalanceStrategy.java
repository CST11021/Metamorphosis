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
package com.taobao.metamorphosis.client.consumer;

import java.util.List;


/**
 * Consumer的balance策略
 *
 * 说到metaq的消费者balance策略，不得不说一下分区的有关信息。一个topic可以划分为n个分区。每个分区是一个有序的、不可变的、顺序递增的队列。
 * 分区一方面是为了增大消息的容量（可以分布在多个分区上存，而不会限制在单台机器存储大小里），二方面可以类似看成一种并行度。
 *
 *         消费者的负载均衡与topic的分区数据紧密相关，需要考虑几种情况：
 *
 *             1、单个分组内的消费者数目如果比总得分区数目多的话，则多出来的消费者不参与消费。每个分区针对每个消费者group只挂一个消费者，同一个group的多余消费者不参与消费。
 *
 *             2、如果分组内的消费者数目比分区数目小，则有部分消费者要额外承担消息的消费任务。当分区数目n大于单个group的消费者数目m时，则有n%m个消费者需要额外承担1/n的消费任务。n足够大的时候可以认为负载平均分配。
 *
 *         综上所述，单个分组内的消费者集群的负载均衡策略如下：
 *
 *             ①每个分区针对一个group只挂载一个消费者
 *
 *             ②如果同一个group的消费者数目大于分区数目，则多出来的消费者不参与消费
 *
 *             ③如果同一个group的消费者数目小于分区数目，则有部分消费者需要额外承担消费任务。
 *
 *         meta客户端处理消费者的负载均衡方式：将消费者列表和分区列表分别排序，然后按照上述规则做合理的挂载。如果某个消费者故障，其他消费者会感知到这一变化，然后重新进行负载均衡，保证所有分区都有消费者进行消费。
 *
 *         Consumer的balance策略实现在metaq中提供了两种：ConsisHashStrategy和DefaultLoadBalanceStrategy。
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface LoadBalanceStrategy {

    enum Type {
        DEFAULT,
        CONSIST
    }

    /**
     * 根据consumer id查找对应的分区列表
     * 
     * @param topic         分区topic
     * @param consumerId    消费者ID，消息消费者的唯一标识
     * @param curConsumers  当前可以进行拉取消息消费的消费者
     * @param curPartitions 当前的分区列表
     * 
     * @return 返回分区列表，即当前的消费者只消费从该接口返回的分区下的消息
     */
    public List<String> getPartitions(String topic, String consumerId, final List<String> curConsumers, final List<String> curPartitions);

}