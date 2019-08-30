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

import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;


/**
 * Topic的订阅者信息
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public class SubscriberInfo {
    /** 消息监听器，用于消费消息 */
    private final MessageListener messageListener;
    /** 自定义的消息过滤器 */
    private final ConsumerMessageFilter consumerMessageFilter;
    /** 订阅每次接收的最大数据大小，即消费者每次请求从MQ服务器拉取多少消息 */
    private final int maxSize;


    public SubscriberInfo(final MessageListener messageListener, final ConsumerMessageFilter consumerMessageFilter, final int maxSize) {
        super();
        this.messageListener = messageListener;
        this.maxSize = maxSize;
        this.consumerMessageFilter = consumerMessageFilter;
    }

    public ConsumerMessageFilter getConsumerMessageFilter() {
        return this.consumerMessageFilter;
    }

    public MessageListener getMessageListener() {
        return this.messageListener;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.consumerMessageFilter == null ? 0 : this.consumerMessageFilter.hashCode());
        result = prime * result + this.maxSize;
        result = prime * result + (this.messageListener == null ? 0 : this.messageListener.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SubscriberInfo other = (SubscriberInfo) obj;
        if (this.consumerMessageFilter == null) {
            if (other.consumerMessageFilter != null) {
                return false;
            }
        }
        else if (!this.consumerMessageFilter.equals(other.consumerMessageFilter)) {
            return false;
        }
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.messageListener == null) {
            if (other.messageListener != null) {
                return false;
            }
        }
        else if (!this.messageListener.equals(other.messageListener)) {
            return false;
        }
        return true;
    }

    public int getMaxSize() {
        return this.maxSize;
    }

}