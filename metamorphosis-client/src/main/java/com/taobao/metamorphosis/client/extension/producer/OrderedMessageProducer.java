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
package com.taobao.metamorphosis.client.extension.producer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.client.producer.SimpleMessageProducer;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.exception.MetaOpeartionTimeoutException;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.utils.HexSupport;


/**
 * 有序消息生产者的实现类,需要按照消息内容(例如某个id)散列到固定分区并要求有序的场景中使用.
 * 当预期的分区不可用时,消息将缓存到本地,分区可用时恢复.
 *
 * Metamorphosis对消息顺序性的保证是有限制的，默认情况下，消息的顺序以谁先达到服务器并写入磁盘，则谁就在先的原则处理。并且，发往同一个分区
 * 的消息保证按照写入磁盘的顺序让消费者消费，这是因为消费者针对每个分区都是按照从前到后递增offset的顺序拉取消息。
 * Meta可以保证，在单线程内使用该producer发送的消息按照发送的顺序达到服务器并存储，并按照相同顺序被消费者消费，前提是这些消息发往同一台服
 * 务器的同一个分区。为了实现这一点，你还需要实现自己的PartitionSelector用于固定选择分区
 *
 * public interface PartitionSelector {
 *     public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
 * }
 * 选择分区可以按照一定的业务逻辑来选择，如根据业务id来取模。或者如果是传输文件，可以固定选择第n个分区使用。当然，如果传输文件，通常我们会建议你只配置一个分区，那也就无需选择了。
 *
 * 消息的顺序发送我们在1.2这个版本提供了OrderedMessageProducer，自定义管理分区信息，并提供故障情况下的本地存储功能。
 * 
 * @author 无花
 * @since 2011-8-24 下午4:37:48
 */
public class OrderedMessageProducer extends SimpleMessageProducer {

    private static final Log log = LogFactory.getLog(OrderedMessageProducer.class);

    private final boolean sendFailAndSaveToLocal = Boolean.parseBoolean(System.getProperty("meta.ordered.saveToLocalWhenFailed", "false"));

    /** 当预期的分区不可用时,消息将缓存到本地,分区可用时恢复 */
    private final MessageRecoverManager localMessageStorageManager;

    /** 有序的消息发送器 */
    private final OrderedMessageSender orderMessageSender;

    /** 用于恢复消息的管理器 */
    private final MessageRecoverManager.MessageRecoverer recoverer = new MessageRecoverManager.MessageRecoverer() {

        @Override
        public void handle(final Message msg) throws Exception {
            final SendResult sendResult = OrderedMessageProducer.this.sendMessageToServer(msg, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
            // 恢复时还是失败,抛出异常停止后续消息的恢复
            if (!sendResult.isSuccess()) {
                throw new MetaClientException(sendResult.getErrorMessage());
            }
        }

    };


    public OrderedMessageProducer(final MetaMessageSessionFactory messageSessionFactory,
                                  final RemotingClientWrapper remotingClient,
                                  final PartitionSelector partitionSelector,
                                  final ProducerZooKeeper producerZooKeeper,
                                  final String sessionId,
                                  final MessageRecoverManager localMessageStorageManager) {
        super(messageSessionFactory, remotingClient, partitionSelector, producerZooKeeper, sessionId);
        this.localMessageStorageManager = localMessageStorageManager;
        this.orderMessageSender = new OrderedMessageSender(this);
    }


    @Override
    public void publish(final String topic) {
        super.publish(topic);
        this.localMessageStorageManager.setMessageRecoverer(this.recoverer);
    }

    @Override
    public SendResult sendMessage(final Message message, final long timeout, final TimeUnit unit) throws MetaClientException, InterruptedException {
        this.checkState();
        this.checkMessage(message);
        return this.orderMessageSender.sendMessage(message, timeout, unit);
    }

    /**
     * 根据消息，选择一个该消息存储的分区
     *
     * @param message
     * @return
     * @throws MetaClientException
     */
    Partition selectPartition(final Message message) throws MetaClientException {
        return this.producerZooKeeper.selectPartition(message.getTopic(), message, this.partitionSelector);
    }

    /**
     * 将消息保存到本地
     *
     * @param message
     * @param partition
     * @param timeout
     * @param unit
     * @return
     */
    SendResult saveMessageToLocal(final Message message, final Partition partition, final long timeout, final TimeUnit unit) {
        try {
            this.localMessageStorageManager.append(message, partition);
            return new SendResult(true, partition, -1, "send to local");
        }
        catch (final IOException e) {
            log.error("send message to local failed,topic=" + message.getTopic() + ",content["
                    + HexSupport.toHexFromBytes(message.getData()) + "]");
            return new SendResult(false, null, -1, "send message to local failed");
        }
    }

    /**
     * 将消息发送到服务器
     *
     * @param message
     * @param timeout
     * @param unit
     * @param saveToLocalWhileForbidden
     * @return
     * @throws MetaClientException
     * @throws InterruptedException
     * @throws MetaOpeartionTimeoutException
     */
    SendResult sendMessageToServer(final Message message, final long timeout, final TimeUnit unit, final boolean saveToLocalWhileForbidden) throws MetaClientException, InterruptedException, MetaOpeartionTimeoutException {
        final SendResult sendResult = this.sendMessageToServer(message, timeout, unit);
        if (this.needSaveToLocalWhenSendFailed(sendResult)
                || this.needSaveToLocalWhenForbidden(saveToLocalWhileForbidden, sendResult)) {
            log.warn("send to server fail,save to local." + sendResult.getErrorMessage());
            return this.saveMessageToLocal(message, Partition.RandomPartiton, timeout, unit);
        }
        else {
            return sendResult;
        }
    }

    /**
     * 用于判断，当消息发送失败时，是否保存到本地
     *
     * @param sendResult
     * @return
     */
    private boolean needSaveToLocalWhenSendFailed(final SendResult sendResult) {
        return !sendResult.isSuccess() && sendFailAndSaveToLocal;
    }

    /**
     * 用于判断，当消息发送时，是否保存到本地
     * @param saveToLocalWhileForbidden
     * @param sendResult
     * @return
     */
    private boolean needSaveToLocalWhenForbidden(final boolean saveToLocalWhileForbidden, final SendResult sendResult) {
        return !sendResult.isSuccess() && sendResult.getErrorMessage().equals(String.valueOf(HttpStatus.Forbidden))
                && saveToLocalWhileForbidden;
    }

    /**
     * 获取本地的消息数量
     *
     * @param topic
     * @param partition
     * @return
     */
    int getLocalMessageCount(final String topic, final Partition partition) {
        return this.localMessageStorageManager.getMessageCount(topic, partition);
    }

    /**
     * 尝试恢复消息
     *
     * @param topic
     * @param partition
     */
    void tryRecoverMessage(final String topic, final Partition partition) {
        this.localMessageStorageManager.recover(topic, partition, this.recoverer);
    }

}