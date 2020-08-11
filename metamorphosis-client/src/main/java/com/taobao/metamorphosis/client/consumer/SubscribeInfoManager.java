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
 * ������Ϣ������������ά���ĸ�topic���ĸ�������������{@link MessageListener}����������������Ϣ
 */
public class SubscribeInfoManager {

    /** Map<group, Map<topic, SubscriberInfo>> ����ά���ĸ�topic���ĸ�������������{@link MessageListener}����������������Ϣ */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> groupTopicSubcriberRegistry = new ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>>();

    /**
     * ����topic
     *
     * @param topic                 ��ʾ�����Ѷ˶��ĵ�topic
     * @param group                 ���Ѷ˵�group����
     * @param maxSize               ���Ѷ˵��ν������ѵ����������
     * @param messageListener       ��Ϣ�����������ڴ���ӷ���˻�ȡ����Ϣ
     * @param consumerMessageFilter �Զ�����Ϣ�������������ص���Ϣ�����þܾ����Խ��д���
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
     * ��ȡgroup�µĶ�����Ϣ
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
     * ��ȡtopic��Ӧ����Ϣ������
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
     * �Ƴ�group����Ķ�����Ϣ
     *
     * @param group
     */
    public void removeGroup(final String group) {
        this.groupTopicSubcriberRegistry.remove(group);
    }

    /**
     * ��ȡ���еĶ�����Ϣ
     *
     * @return
     */
    ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> getGroupTopicSubcriberRegistry() {
        return this.groupTopicSubcriberRegistry;
    }
}