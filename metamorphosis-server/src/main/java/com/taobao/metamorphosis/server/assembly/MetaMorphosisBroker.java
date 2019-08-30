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
package com.taobao.metamorphosis.server.assembly;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.config.ServerConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.TransactionCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.filter.ConsumerFilterManager;
import com.taobao.metamorphosis.server.network.GetProcessor;
import com.taobao.metamorphosis.server.network.OffsetProcessor;
import com.taobao.metamorphosis.server.network.PutProcessor;
import com.taobao.metamorphosis.server.network.QuitProcessor;
import com.taobao.metamorphosis.server.network.StatsProcessor;
import com.taobao.metamorphosis.server.network.TransactionProcessor;
import com.taobao.metamorphosis.server.network.VersionProcessor;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.DeletePolicy;
import com.taobao.metamorphosis.server.store.DeletePolicyFactory;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.MetaMBeanServer;
import com.taobao.metamorphosis.utils.IdWorker;

/**
 * ��װ��meta server
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class MetaMorphosisBroker implements MetaMorphosisBrokerMBean {

    static final Log log = LogFactory.getLog(MetaMorphosisBroker.class);

    /**
     * �����������˳�,ϵͳ���� System.exit��������������ر�ʱ�Ż�ִ����ӵ�shutdownHook�̡߳�
     * ����shutdownHook��һ���ѳ�ʼ������û���������̣߳���jvm�رյ�ʱ�򣬻�ִ��ϵͳ���Ѿ����õ�����ͨ������addShutdownHook��ӵĹ��ӣ�
     * ��ϵͳִ������Щ���Ӻ�jvm�Ż�رա����Կ�ͨ����Щ������jvm�رյ�ʱ������ڴ�������Դ���յȹ�����
     */
    private final class ShutdownHook extends Thread {
        @Override
        public void run() {
            MetaMorphosisBroker.this.runShutdownHook = true;
            MetaMorphosisBroker.this.stop();
        }
    }

    /** ֹͣ����Ĺ��Ӷ��� */
    private final ShutdownHook shutdownHook;
    /** ���ڱ�ʶ�Ƿ�������ֹͣ����Ĺ��ӷ��� */
    private volatile boolean runShutdownHook = false;
    /** MQ����Ϣ�洢������ */
    private final MessageStoreManager storeManager;
    /** ���ڹ�����get��put������̳߳� */
    private final ExecutorsManager executorsManager;
    /** ͳ�ƹ����� */
    private final StatsManager statsManager;
    /** ����ͨѶ��MQ���� */
    private final RemotingServer remotingServer;
    /** MQ��صĲ������� */
    private final MetaConfig metaConfig;
    /** ���ڴ���ID�Ķ��� */
    private final IdWorker idWorker;
    /** Broker��ZK����������ע��broker��topic�� */
    private final BrokerZooKeeper brokerZooKeeper;
    private final ConsumerFilterManager consumerFilterManager;
    /** MQ������������ */
    private CommandProcessor brokerProcessor;
    /** ���ڱ�ʶ�����Ƿ�ر� */
    private boolean shutdown = true;
    /** ��ʾ��ǰbroker�Ƿ�ɹ�ע�ᵽzk����ǰMQ��������ʱ���Ὣbroker�����Ϣע�ᵽzk */
    private boolean registerZkSuccess;



    public MetaMorphosisBroker(final MetaConfig metaConfig) {
        super();
        // ע��ֹͣ����Ĺ��ӷ���
        this.shutdownHook = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);

        this.metaConfig = metaConfig;
        // ����һ������ͨѶ��RemotingServer��������ͨѶ����õİ����gecko���
        this.remotingServer = newRemotingServer(metaConfig);
        // �̳߳ط������������������MQ������Ϣ������������ߴ�MQ��ȡ��Ϣ��������ͨ���ù������ṩ���߳̽��д���ģ�
        this.executorsManager = new ExecutorsManager(metaConfig);
        // ��������Ψһ����ϢID��ȫ��Ψһ��ʱ������
        this.idWorker = new IdWorker(metaConfig.getBrokerId());
        // ��Ϣ�洢������
        this.storeManager = new MessageStoreManager(metaConfig, this.newDeletePolicy(metaConfig));
        // ͳ�ƹ�����
        this.statsManager = new StatsManager(this.metaConfig, this.storeManager, this.remotingServer);
        // Broker��ZK������ע��broker��topic��
        this.brokerZooKeeper = new BrokerZooKeeper(metaConfig);

        // ��ʼ������洢����
        JournalTransactionStore transactionStore = null;
        try {
            transactionStore = new JournalTransactionStore(metaConfig.getDataLogPath(), this.storeManager, metaConfig);
        } catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing transaction store failed", e);
        }

        // ��������ڹ������topic-consumerGroup����Ϣ������
        try {
            this.consumerFilterManager = new ConsumerFilterManager(metaConfig);
        } catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing ConsumerFilterManager failed", e);
        }

        // meta������������
        final BrokerCommandProcessor next = new BrokerCommandProcessor(
                this.storeManager, this.executorsManager, this.statsManager,
                this.remotingServer, metaConfig, this.idWorker, this.brokerZooKeeper, this.consumerFilterManager);

        // �����������
        this.brokerProcessor = new TransactionalCommandProcessor(
                metaConfig, this.storeManager, this.idWorker, next, transactionStore, this.statsManager);

        MetaMBeanServer.registMBean(this, null);
    }

    /** ����MetaQ���� */
    public synchronized void start() {
        if (!this.shutdown) {
            return;
        }
        this.shutdown = false;

        // ��ʼ����Ϣ�洢������
        this.storeManager.init();
        //
        this.executorsManager.init();
        // ͳ�ƹ�����
        this.statsManager.init();
        // ע���������
        this.registerProcessors();

        try {
            this.remotingServer.start();
        } catch (final NotifyRemotingException e) {
            throw new MetamorphosisServerStartupException("start remoting server failed", e);
        }

        // ����ǰbroker�����Ϣע�ᵽzk
        try {
            // ע��broker��zk
            this.brokerZooKeeper.registerBrokerInZk();
            // ����ǰbroker����һ�� master_config_checksum �ڵ㵽zk
            this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
            // ���topic���������
            this.addTopicsChangeListener();
            // ע�ᵱǰbroker��topic��Ϣ��zk
            this.registerTopicsInZk();
            this.registerZkSuccess = true;
        }
        catch (final Exception e) {
            this.registerZkSuccess = false;
            throw new MetamorphosisServerStartupException("Register broker to zk failed", e);
        }

        log.info("Starting metamorphosis server...");
        this.brokerProcessor.init();
        log.info("Start metamorphosis server successfully");
    }

    /** ֹͣMetaQ���� */
    @Override
    public synchronized void stop() {
        if (this.shutdown) {
            return;
        }

        log.info("Stopping metamorphosis server...");
        this.shutdown = true;
        // ����broker��zk��ע��
        this.brokerZooKeeper.close(this.registerZkSuccess);
        try {
            // Waiting for zookeeper to notify clients.
            Thread.sleep(this.brokerZooKeeper.getZkConfig().zkSyncTimeMs);
        }
        catch (InterruptedException e) {
            // ignore
        }
        this.executorsManager.dispose();
        this.storeManager.dispose();
        this.statsManager.dispose();
        try {
            this.remotingServer.stop();
        }
        catch (final NotifyRemotingException e) {
            log.error("Shutdown remoting server failed", e);
        }

        // ���ӷ���ִ����ɺ�Ҫ��JVM�Ƴ�
        if (!this.runShutdownHook && this.shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
            }
            catch (Exception e) {
                // ignore
            }
        }

        this.brokerProcessor.dispose();
        // ֹͣ��Ƕzk server
        EmbedZookeeperServer.getInstance().stop();

        log.info("Stop metamorphosis server successfully");
    }

    /**
     * �������ô���һ��ɾ����Ϣ�ļ��Ĳ���ʵ��
     * @param metaConfig
     * @return
     */
    private DeletePolicy newDeletePolicy(final MetaConfig metaConfig) {
        final String deletePolicy = metaConfig.getDeletePolicy();
        if (deletePolicy != null) {
            return DeletePolicyFactory.getDeletePolicy(deletePolicy);
        }
        return null;
    }

    /**
     * ����������Ϣ����һ������ͨѶ�����{@link RemotingServer}����
     * @param metaConfig MQ���ö���
     * @return
     */
    private static RemotingServer newRemotingServer(final MetaConfig metaConfig) {
        final ServerConfig serverConfig = new ServerConfig();
        serverConfig.setWireFormatType(new MetamorphosisWireFormatType());
        serverConfig.setPort(metaConfig.getServerPort());
        final RemotingServer server = RemotingFactory.newRemotingServer(serverConfig);
        return server;
    }

    /**
     * ע������������ͻ��˻���MQ���͸������󣬰�����GetCommand��PutCommand��OffsetCommand��HeartBeatRequestCommand��QuitCommand��StatsCommand��TransactionCommand
     * ÿ�����͵������ж�Ӧ�Ĵ�����������
     */
    private void registerProcessors() {
        this.remotingServer.registerProcessor(GetCommand.class, new GetProcessor(this.brokerProcessor, this.executorsManager.getGetExecutor()));
        this.remotingServer.registerProcessor(PutCommand.class, new PutProcessor(this.brokerProcessor, this.executorsManager.getUnOrderedPutExecutor()));
        this.remotingServer.registerProcessor(OffsetCommand.class, new OffsetProcessor(this.brokerProcessor, this.executorsManager.getGetExecutor()));
        this.remotingServer.registerProcessor(HeartBeatRequestCommand.class, new VersionProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(QuitCommand.class, new QuitProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(StatsCommand.class, new StatsProcessor(this.brokerProcessor));
        this.remotingServer.registerProcessor(TransactionCommand.class, new TransactionProcessor(this.brokerProcessor, this.executorsManager.getUnOrderedPutExecutor()));
    }

    /**
     * ���topic���������������ǰ��MQ��������topic���ʱ��ͬ����zk��
     */
    private void addTopicsChangeListener() {
        // ����topics�б�仯��ע�ᵽzk
        this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                try {
                    MetaMorphosisBroker.this.registerTopicsInZk();
                }
                catch (final Exception e) {
                    log.error("Register topic in zk failed", e);
                }
            }
        });
    }

    /**
     * ע�ᵱǰbroker��topic��Ϣ��zk
     * @throws Exception
     */
    private void registerTopicsInZk() throws Exception {
        // 1����ע�������е�topic��zookeeper
        for (final String topic : this.metaConfig.getTopics()) {
            this.brokerZooKeeper.registerTopicInZk(topic, true);
        }
        // 2��ע����Ϣ�������е�topic��zookeeper
        for (final String topic : this.storeManager.getMessageStores().keySet()) {
            this.brokerZooKeeper.registerTopicInZk(topic, true);
        }
    }

    public void setBrokerProcessor(final CommandProcessor brokerProcessor) {
        this.brokerProcessor = brokerProcessor;
    }

    // ----------
    // getter ...
    // ----------

    public CommandProcessor getBrokerProcessor() {
        return this.brokerProcessor;
    }
    public MetaConfig getMetaConfig() {
        return this.metaConfig;
    }
    public synchronized boolean isShutdown() {
        return this.shutdown;
    }
    public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }
    public ExecutorsManager getExecutorsManager() {
        return this.executorsManager;
    }
    public StatsManager getStatsManager() {
        return this.statsManager;
    }
    public RemotingServer getRemotingServer() {
        return this.remotingServer;
    }
    public ConsumerFilterManager getConsumerFilterManager() {
        return this.consumerFilterManager;
    }
    public IdWorker getIdWorker() {
        return this.idWorker;
    }
    public BrokerZooKeeper getBrokerZooKeeper() {
        return this.brokerZooKeeper;
    }

}