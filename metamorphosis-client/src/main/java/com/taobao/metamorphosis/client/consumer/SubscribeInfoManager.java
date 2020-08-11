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

import java.util.concurrent.ConcurrentHashMap;

import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 订阅信息管理器：用于维护哪个topic被哪个监听器监听，{@link MessageListener}监听器用于消费消息
 */
public class SubscribeInfoManager {

    /** Map<group, Map<topic, SubscriberInfo>> 用于维护哪个topic被哪个监听器监听，{@link MessageListener}监听器用于消费消息 */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> groupTopicSubcriberRegistry = new ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>>();

    /**
     * 订阅topic
     *
     * @param topic                 表示被消费端订阅的topic
     * @param group                 消费端的group分组
     * @param maxSize               消费端单次接收消费的最大数据量
     * @param messageListener       消息监听器，用于处理从服务端获取的消息
     * @param consumerMessageFilter 自定义消息过滤器，被拦截的消息将调用拒绝策略进行处理
     * @throws MetaClientException
     */
    public void subscribe(final String topic, final String group, final int maxSize, final MessageListener messageListener, final ConsumerMessageFilter consumerMessageFilter) throws MetaClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry = this.getTopicSubscriberRegistry(group);
        SubscriberInfo info = topicSubsriberRegistry.get(topic);
        if (info == null) {
            info = new SubscriberInfo(messageListener, consumerMessageFilter, maxSize);
            final SubscriberInfo oldInfo = topicSubsriberRegistry.putIfAbsent(topic, info);
            if (oldInfo != null) {
                throw new MetaClientException("Topic=" + topic + " has been subscribered by group " + group);
            }
        }
        else {
            throw new MetaClientException("Topic=" + topic + " has been subscribered by group " + group);
        }
    }

    /**
     * 获取group下的订阅信息
     *
     * @param group
     * @return Map<topic, SubscriberInfo>
     * @throws MetaClientException
     */
    private ConcurrentHashMap<String, SubscriberInfo> getTopicSubscriberRegistry(final String group) throws MetaClientException {
        // Map<Topic, SubscriberInfo>
        ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry = this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            topicSubsriberRegistry = new ConcurrentHashMap<String, SubscriberInfo>();
            final ConcurrentHashMap<String/* topic */, SubscriberInfo> old =
                    this.groupTopicSubcriberRegistry.putIfAbsent(group, topicSubsriberRegistry);
            if (old != null) {
                topicSubsriberRegistry = old;
            }
        }
        return topicSubsriberRegistry;
    }

    /**
     * 获取topic对应的消息监听器
     *
     * @param topic
     * @param group
     * @return
     * @throws MetaClientException
     */
    public MessageListener getMessageListener(final String topic, final String group) throws MetaClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry = this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            return null;
        }
        final SubscriberInfo info = topicSubsriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getMessageListener();
    }

    /**
     * 移除group分组的订阅信息
     *
     * @param group
     */
    public void removeGroup(final String group) {
        this.groupTopicSubcriberRegistry.remove(group);
    }

    /**
     * 获取所有的订阅信息
     *
     * @return
     */
    ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> getGroupTopicSubcriberRegistry() {
        return this.groupTopicSubcriberRegistry;
    }
}