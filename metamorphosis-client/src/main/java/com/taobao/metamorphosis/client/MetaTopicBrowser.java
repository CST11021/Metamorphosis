package com.taobao.metamorphosis.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 每个topic对应一个MetaTopicBrowser实例，用于查看指定topic下的所有分区和消息
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 */
public class MetaTopicBrowser implements TopicBrowser {

    /** 表示该MetaTopicBrowser实例对应的topic */
    private final String topic;

    /** 消息消费者 */
    private final MessageConsumer consumer;

    /** 表示该topic下的所有可用分区 */
    private final List<Partition> partitions;

    /** 表示该抓取请求，每次从MQ服务器拉取多少消息 */
    private final int maxSize;

    /** 从MQ服务器抓取消息的超时时间 */
    private final long timeoutInMills;

    /**
     * 用于迭代消息的迭代器
     */
    protected class Itr implements Iterator<Message> {
        /** 表示该迭代器要迭代的分区列表 */
        protected final List<Partition> partitions;
        /** 消息迭代器，解析传输过来的数据，消费者每次从MQ拉取消息后，都会封装为一个MessageIterator对象，该对象表示一连串的连续消息集合，也就是说消费者每次从服务拉取消息都是批量拉取的 */
        private MessageIterator it;
        /** 当前迭代的消息偏移量 */
        private long offset = 0L;
        /** 当前迭代的分区 */
        private Partition partition;


        public Itr(List<Partition> partitions) {
            super();
            this.partitions = partitions;
        }


        @Override
        public boolean hasNext() {
            try {
                if (this.it != null && this.it.hasNext()) {
                    return true;
                }
                else {
                    if (this.partition == null) {
                        if (this.partitions.isEmpty()) {
                            return false;
                        }
                        else {
                            this.nextPartition();
                        }
                    }
                    while (this.partition != null) {
                        // 如果此分区不为null，则增加偏移量。
                        if (this.it != null) {
                            this.offset += this.it.getOffset();
                        }
                        this.it = MetaTopicBrowser.this.consumer.get(
                                MetaTopicBrowser.this.topic,
                                this.partition,
                                this.offset,
                                MetaTopicBrowser.this.maxSize,
                                MetaTopicBrowser.this.timeoutInMills,
                                TimeUnit.MILLISECONDS);
                        if (this.it != null && this.it.hasNext()) {
                            // 如果此分区仍然有消息，则返回true。
                            return true;
                        }
                        else {
                            // 移至下一个分区。
                            if (this.partitions.isEmpty()) {
                                this.partition = null;
                                // 没有更多分区，返回false。
                                return false;
                            }
                            else {
                                // 更改分区，然后继续尝试获取迭代器。
                                this.nextPartition();
                            }
                        }
                    }
                    return false;

                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }


        private void nextPartition() {
            this.partition = this.partitions.get(0);
            this.partitions.remove(0);
            this.offset = 0;
            this.it = null;
        }


        @Override
        public Message next() {
            if (this.hasNext()) {
                try {
                    return this.it.next();
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            else {
                throw new NoSuchElementException();
            }
        }


        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }


    public MessageConsumer getConsumer() {
        return this.consumer;
    }


    public MetaTopicBrowser(String topic, int maxSize, long timeoutInMills, MessageConsumer consumer, List<Partition> partitions) {
        super();
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("Blank topic");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Invalid max size");
        }
        if (timeoutInMills <= 0) {
            throw new IllegalArgumentException("Invalid timeout value");
        }
        this.timeoutInMills = timeoutInMills;
        this.topic = topic;
        this.maxSize = maxSize;
        this.consumer = consumer;
        this.partitions = partitions;
    }


    @Override
    public Iterator<Message> iterator() {
        return new Itr(new ArrayList<Partition>(this.partitions));
    }


    @Override
    public List<Partition> getPartitions() {
        return Collections.unmodifiableList(this.partitions);
    }


    @Override
    public void shutdown() throws MetaClientException {
        this.consumer.shutdown();
    }


    @Override
    public String getTopic() {
        return this.topic;
    }

}
