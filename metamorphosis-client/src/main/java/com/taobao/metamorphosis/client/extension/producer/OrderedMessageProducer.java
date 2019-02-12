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
 * ������Ϣ�����ߵ�ʵ����,��Ҫ������Ϣ����(����ĳ��id)ɢ�е��̶�������Ҫ������ĳ�����ʹ��.
 * ��Ԥ�ڵķ���������ʱ,��Ϣ�����浽����,��������ʱ�ָ�.
 *
 * Metamorphosis����Ϣ˳���Եı�֤�������Ƶģ�Ĭ������£���Ϣ��˳����˭�ȴﵽ��������д����̣���˭�����ȵ�ԭ�������ң�����ͬһ������
 * ����Ϣ��֤����д����̵�˳�������������ѣ�������Ϊ���������ÿ���������ǰ��մ�ǰ�������offset��˳����ȡ��Ϣ��
 * Meta���Ա�֤���ڵ��߳���ʹ�ø�producer���͵���Ϣ���շ��͵�˳��ﵽ���������洢����������ͬ˳�����������ѣ�ǰ������Щ��Ϣ����ͬһ̨��
 * ������ͬһ��������Ϊ��ʵ����һ�㣬�㻹��Ҫʵ���Լ���PartitionSelector���ڹ̶�ѡ�����
 *
 * public interface PartitionSelector {
 *     public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
 * }
 * ѡ��������԰���һ����ҵ���߼���ѡ�������ҵ��id��ȡģ����������Ǵ����ļ������Թ̶�ѡ���n������ʹ�á���Ȼ����������ļ���ͨ�����ǻὨ����ֻ����һ����������Ҳ������ѡ���ˡ�
 *
 * ��Ϣ��˳����������1.2����汾�ṩ��OrderedMessageProducer���Զ�����������Ϣ�����ṩ��������µı��ش洢���ܡ�
 * 
 * @author �޻�
 * @since 2011-8-24 ����4:37:48
 */
public class OrderedMessageProducer extends SimpleMessageProducer {

    private static final Log log = LogFactory.getLog(OrderedMessageProducer.class);

    private final boolean sendFailAndSaveToLocal = Boolean.parseBoolean(System.getProperty("meta.ordered.saveToLocalWhenFailed", "false"));

    /** ��Ԥ�ڵķ���������ʱ,��Ϣ�����浽����,��������ʱ�ָ� */
    private final MessageRecoverManager localMessageStorageManager;

    /** �������Ϣ������ */
    private final OrderedMessageSender orderMessageSender;

    /** ���ڻָ���Ϣ�Ĺ����� */
    private final MessageRecoverManager.MessageRecoverer recoverer = new MessageRecoverManager.MessageRecoverer() {

        @Override
        public void handle(final Message msg) throws Exception {
            final SendResult sendResult = OrderedMessageProducer.this.sendMessageToServer(msg, DEFAULT_OP_TIMEOUT, TimeUnit.MILLISECONDS);
            // �ָ�ʱ����ʧ��,�׳��쳣ֹͣ������Ϣ�Ļָ�
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
     * ������Ϣ��ѡ��һ������Ϣ�洢�ķ���
     *
     * @param message
     * @return
     * @throws MetaClientException
     */
    Partition selectPartition(final Message message) throws MetaClientException {
        return this.producerZooKeeper.selectPartition(message.getTopic(), message, this.partitionSelector);
    }

    /**
     * ����Ϣ���浽����
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
     * ����Ϣ���͵�������
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
     * �����жϣ�����Ϣ����ʧ��ʱ���Ƿ񱣴浽����
     *
     * @param sendResult
     * @return
     */
    private boolean needSaveToLocalWhenSendFailed(final SendResult sendResult) {
        return !sendResult.isSuccess() && sendFailAndSaveToLocal;
    }

    /**
     * �����жϣ�����Ϣ����ʱ���Ƿ񱣴浽����
     * @param saveToLocalWhileForbidden
     * @param sendResult
     * @return
     */
    private boolean needSaveToLocalWhenForbidden(final boolean saveToLocalWhileForbidden, final SendResult sendResult) {
        return !sendResult.isSuccess() && sendResult.getErrorMessage().equals(String.valueOf(HttpStatus.Forbidden))
                && saveToLocalWhileForbidden;
    }

    /**
     * ��ȡ���ص���Ϣ����
     *
     * @param topic
     * @param partition
     * @return
     */
    int getLocalMessageCount(final String topic, final Partition partition) {
        return this.localMessageStorageManager.getMessageCount(topic, partition);
    }

    /**
     * ���Իָ���Ϣ
     *
     * @param topic
     * @param partition
     */
    void tryRecoverMessage(final String topic, final Partition partition) {
        this.localMessageStorageManager.recover(topic, partition, this.recoverer);
    }

}