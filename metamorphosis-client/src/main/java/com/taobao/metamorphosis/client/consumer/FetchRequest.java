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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 表示一次从MQ服务器抓取消息的请求
 * 
 * @author boyan
 * @Date 2011-4-25
 * 
 */
public class FetchRequest implements Delayed {

    static final Log log = LogFactory.getLog(FetchRequest.class);

    /** 延后的时间戳，当前该抓取请求的对象被保存到队列后，通过设置该属性来实现延迟队列，即该请求对象被保存多久后才能从队列里取出 */
    private long delayTimeStamp;
    private long delay;
    /** 表示该请求从MQ服务器抓取消息的所在分区信息 */
    private TopicPartitionRegInfo topicPartitionRegInfo;
    /** 表示该抓取请求，每次从MQ服务器拉取多少消息 */
    private int maxSize;
    private int originalMaxSize;
    /** 表示该请求指向的MQ服务器 */
    private Broker broker;
    /** 表示这个请求重新投递的次数 */
    private int retries = 0;
    private long tmpOffset;
    /** 表示该请求对象要保存到的队列 */
    private FetchRequestQueue refQueue;

    /**
     * Just for test
     *
     * @param delay
     */
    FetchRequest(final long delay) {
        super();
        this.delay = delay;
        this.delayTimeStamp = System.currentTimeMillis() + delay;
    }
    public FetchRequest(final Broker broker, final long delay, final TopicPartitionRegInfo topicPartitionRegInfo, final int maxSize) {
        super();
        this.broker = broker;
        this.delay = delay;
        if (delay >= 0) {
            this.delayTimeStamp = System.currentTimeMillis() + delay;
        }
        this.topicPartitionRegInfo = topicPartitionRegInfo;
        this.maxSize = maxSize;
        this.originalMaxSize = maxSize;
        if (this.maxSize <= 0) {
            throw new IllegalArgumentException("maxSize <=0");
        }
    }



    public TopicPartitionRegInfo getTopicPartitionRegInfo() {
        return this.topicPartitionRegInfo;
    }
    public FetchRequestQueue getRefQueue() {
        return this.refQueue;
    }
    public void setRefQueue(FetchRequestQueue refQueue) {
        this.refQueue = refQueue;
    }
    public int getRetries() {
        return this.retries;
    }

    public void increaseMaxSize() {
        if (this.maxSize > MessageUtils.MAX_READ_BUFFER_SIZE) {
            log.warn("警告：maxSize超过最大限制" + MessageUtils.MAX_READ_BUFFER_SIZE
                + "Bytes，请设置环境变量-Dnotify.remoting.max_read_buffer_size超过此限制");
            return;
        }
        this.maxSize = 2 * this.maxSize;
    }

    /**
     * 拉取完消息后，如果发现，得到的消息量小于请求时的消息量大小的1/2时，会设置maxSize为原来的一半
     */
    public void decreaseMaxSize() {
        if (this.maxSize < this.originalMaxSize) {
            this.maxSize = this.originalMaxSize;
        }
        else if (this.maxSize == this.originalMaxSize) {
            return;
        }
        else {
            this.maxSize = this.maxSize / 2;
        }
    }

    public void resetRetries() {
        this.retries = 0;
    }

    public int incrementRetriesAndGet() {
        return ++this.retries;
    }


    public Broker getBroker() {
        return this.broker;
    }
    public void setBroker(final Broker broker) {
        this.broker = broker;
    }

    public long getDelay() {
        return this.delay;
    }
    @Override
    public long getDelay(final TimeUnit unit) {
        return unit.convert(this.delayTimeStamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
    /**
     * 设置延后的时间，单位毫秒
     *
     * @param delay
     */
    public void setDelay(final long delay) {
        this.delay = delay;
        this.delayTimeStamp = System.currentTimeMillis() + delay;
    }

    public int getMaxSize() {
        return this.maxSize;
    }

    /**
     * 返回将要使用的offset，如果有临时offset，则优先使用临时offset
     *
     * @return
     */
    public long getOffset() {
        if (this.tmpOffset > 0) {
            return this.tmpOffset;
        } else {
            return this.topicPartitionRegInfo.getOffset().get();
        }
    }
    /**
     * 更新offset，当ack为true则更新存储中的offset，并将临时offset设置为－1,否则仅更新临时offset
     * 
     * @param offset
     * @param ack
     */
    public void setOffset(final long offset, final long msgId, final boolean ack) {
        if (ack) {
            // 对topicPartitionRegInfo加锁，防止提交到zk不一致
            synchronized (this.topicPartitionRegInfo) {
                this.topicPartitionRegInfo.getOffset().set(offset);
                if (msgId != -1) {
                    this.topicPartitionRegInfo.setMessageId(msgId);
                }
                // 有变更，需要更新到storage
                this.topicPartitionRegInfo.setModified(true);
            }
            this.rollbackOffset();
        } else {
            this.tmpOffset = offset;
        }
    }


    public String getTopic() {
        return this.topicPartitionRegInfo.getTopic();
    }

    public int getPartition() {
        return this.topicPartitionRegInfo.getPartition().getPartition();
    }

    public Partition getPartitionObject() {
        return this.topicPartitionRegInfo.getPartition();

    }

    public void rollbackOffset() {
        this.tmpOffset = -1;
    }

    public long getLastMessageId() {
        return this.topicPartitionRegInfo.getMessageId();
    }




    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.broker == null ? 0 : this.broker.hashCode());
        result = prime * result + (int) (this.delay ^ this.delay >>> 32);
        result = prime * result + this.maxSize;
        result = prime * result + this.retries;
        result = prime * result + (this.topicPartitionRegInfo == null ? 0 : this.topicPartitionRegInfo.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final FetchRequest other = (FetchRequest) obj;
        if (this.broker == null) {
            if (other.broker != null) {
                return false;
            }
        }
        else if (!this.broker.equals(other.broker)) {
            return false;
        }
        if (this.delay != other.delay) {
            return false;
        }
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.retries != other.retries) {
            return false;
        }
        if (this.topicPartitionRegInfo == null) {
            if (other.topicPartitionRegInfo != null) {
                return false;
            }
        }
        else if (!this.topicPartitionRegInfo.equals(other.topicPartitionRegInfo)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(final Delayed o) {
        if (o == this) {
            return 0;
        }
        final FetchRequest other = (FetchRequest) o;
        final long sub = this.delayTimeStamp - other.delayTimeStamp;
        if (sub == 0) {
            return 0;
        }
        else {
            return sub < 0 ? -1 : 1;
        }
    }

}