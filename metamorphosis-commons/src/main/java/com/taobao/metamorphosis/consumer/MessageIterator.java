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
package com.taobao.metamorphosis.consumer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.utils.MessageUtils;


/**
 * 消息迭代器，解析传输过来的数据
 * 消费者每次从MQ拉取消息后，都会封装为一个MessageIterator对象，该对象表示一连串的连续消息集合，也就是说消费者每次从服务拉取消息都是批量拉取的
 * 
 * @author boyan
 * @Date 2011-4-20
 * 
 */
public class MessageIterator {
    /** 消息所属的topic */
    private final String topic;
    /** 表示一次拉取的所有的消息数据 */
    private final byte[] data;
    /** 表示当前迭代的偏移量，默认从0开始 */
    private int offset;
    /** 表示当前偏移量对应的消息对象，比如偏移量是0，则该对象表示第一个消息对象 */
    private Message message;
    /** 表示当前偏移量对应的消息字节 */
    private ByteBuffer currentMsgBuf;


    public MessageIterator(final String topic, final byte[] data) {
        super();
        this.topic = topic;
        this.data = data;
        this.offset = 0;
    }

    /**
     * 当还有消息的时候返回true
     * 
     * @return
     */
    public boolean hasNext() {
        if (this.data == null || this.data.length == 0) {
            return false;
        }
        if (this.offset >= this.data.length) {
            return false;
        }
        if (this.data.length - this.offset < MessageUtils.HEADER_LEN) {
            return false;
        }
        final int msgLen = MessageUtils.getInt(this.offset, this.data);
        if (this.data.length - this.offset - MessageUtils.HEADER_LEN < msgLen) {
            return false;
        }
        return true;

    }

    /**
     * 返回下一个消息
     * 
     * @return
     * @throws InvalidMessageException
     */
    public Message next() throws InvalidMessageException {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        final MessageUtils.DecodedMessage decodeMessage = MessageUtils.decodeMessage(this.topic, this.data, this.offset);
        this.setOffset(decodeMessage.newOffset);
        this.message = decodeMessage.message;
        this.currentMsgBuf = decodeMessage.buf;
        return decodeMessage.message;
    }

    public void remove() {
        throw new UnsupportedOperationException("Unsupported remove");
    }

    /**
     * 获取当前消息的字节数据
     * @return
     */
    public ByteBuffer getCurrentMsgBuf() {
        return this.currentMsgBuf;
    }

    /**
     * 获取消息集合的长度
     *
     * @return
     */
    public int getDataLength() {
        return this.data != null ? this.data.length : 0;
    }

    /**
     * 获取上一个消息(这里是相对于偏移量的)
     *
     * @return
     */
    public Message getPrevMessage() {
        return this.message;
    }
    public int getOffset() {
        return this.offset;
    }
    public void setOffset(final int offset) {
        this.offset = offset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(this.data);
        result = prime * result + this.offset;
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
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
        final MessageIterator other = (MessageIterator) obj;
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        }
        else if (!this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }

}