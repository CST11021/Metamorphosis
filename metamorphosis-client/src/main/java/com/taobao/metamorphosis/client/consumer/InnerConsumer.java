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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 不对外提供的consumer接口，用于提供给Fetch使用
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-9-13
 * 
 */
public interface InnerConsumer {

    /**
     * 从broker抓取消息
     * 
     * @param fetchRequest          抓取请求对象
     * @param timeout               抓取消息的超时时间，默认10秒
     * @param timeUnit              抓取消息的超时时间单位
     * @return
     * @throws MetaClientException
     * @throws InterruptedException
     */
    MessageIterator fetch(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit) throws MetaClientException, InterruptedException;

    /**
     * 返回topic对应的消息处理器，consumer通过MessageListener接口来处理消息
     * 
     * @param topic
     * @return
     */
    MessageListener getMessageListener(final String topic);

    /**
     * 返回topic对应的消息过滤器
     *
     * @param topic
     * @return
     */
    ConsumerMessageFilter getMessageFilter(final String topic);

    /**
     * 获取consumer配置
     *
     * @return
     */
    ConsumerConfig getConsumerConfig();

    /**
     * 无法被消费者处理的消息，会调用该方法
     * 
     * @param message
     * @throws IOException
     */
    void appendCouldNotProcessMessage(final Message message) throws IOException;

    /**
     * 查询offset
     * 
     * @param fetchRequest
     * @return
     * @throws MetaClientException
     */
    long offset(final FetchRequest fetchRequest) throws MetaClientException;

}