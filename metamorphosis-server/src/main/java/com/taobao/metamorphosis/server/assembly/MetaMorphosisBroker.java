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
 * 组装的meta server
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class MetaMorphosisBroker implements MetaMorphosisBrokerMBean {

    static final Log log = LogFactory.getLog(MetaMorphosisBroker.class);

    /**
     * 当程序正常退出,系统调用 System.exit方法或虚拟机被关闭时才会执行添加的shutdownHook线程。
     * 其中shutdownHook是一个已初始化但并没有启动的线程，当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子，
     * 当系统执行完这些钩子后，jvm才会关闭。所以可通过这些钩子在jvm关闭的时候进行内存清理、资源回收等工作。
     */
    private final class ShutdownHook extends Thread {
        @Override
        public void run() {
            MetaMorphosisBroker.this.runShutdownHook = true;
            MetaMorphosisBroker.this.stop();
        }
    }

    /** 停止服务的钩子对象 */
    private final ShutdownHook shutdownHook;
    /** 用于标识是否运行了停止服务的钩子方法 */
    private volatile boolean runShutdownHook = false;
    /** MQ的消息存储管理器 */
    private final MessageStoreManager storeManager;
    /** 用于管理处理get和put请求的线程池 */
    private final ExecutorsManager executorsManager;
    /** 统计管理器 */
    private final StatsManager statsManager;
    /** 用于通讯的MQ服务 */
    private final RemotingServer remotingServer;
    /** MQ相关的参数配置 */
    private final MetaConfig metaConfig;
    /** 用于创建ID的对象 */
    private final IdWorker idWorker;
    /** Broker与ZK交互：用于注册broker和topic等 */
    private final BrokerZooKeeper brokerZooKeeper;
    private final ConsumerFilterManager consumerFilterManager;
    /** MQ服务端命令处理器 */
    private CommandProcessor brokerProcessor;
    /** 用于标识服务是否关闭 */
    private boolean shutdown = true;
    /** 表示当前broker是否成功注册到zk，当前MQ服务启动时，会将broker相关信息注册到zk */
    private boolean registerZkSuccess;



    public MetaMorphosisBroker(final MetaConfig metaConfig) {
        super();
        // 注册停止服务的钩子方法
        this.shutdownHook = new ShutdownHook();
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);

        this.metaConfig = metaConfig;
        // 创建一个用于通讯的RemotingServer对象，这里通讯框架用的阿里的gecko框架
        this.remotingServer = newRemotingServer(metaConfig);
        // 线程池服务管理器（生产者向MQ发送消息的请求和消费者从MQ拉取消息的请求都是通过该管理器提供的线程进行处理的）
        this.executorsManager = new ExecutorsManager(metaConfig);
        // 用于生产唯一的消息ID，全局唯一，时间有序
        this.idWorker = new IdWorker(metaConfig.getBrokerId());
        // 消息存储管理器
        this.storeManager = new MessageStoreManager(metaConfig, this.newDeletePolicy(metaConfig));
        // 统计管理器
        this.statsManager = new StatsManager(this.metaConfig, this.storeManager, this.remotingServer);
        // Broker与ZK交互，注册broker和topic等
        this.brokerZooKeeper = new BrokerZooKeeper(metaConfig);

        // 初始化事务存储引擎
        JournalTransactionStore transactionStore = null;
        try {
            transactionStore = new JournalTransactionStore(metaConfig.getDataLogPath(), this.storeManager, metaConfig);
        } catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing transaction store failed", e);
        }

        // 服务端用于管理各个topic-consumerGroup的消息过滤器
        try {
            this.consumerFilterManager = new ConsumerFilterManager(metaConfig);
        } catch (final Exception e) {
            throw new MetamorphosisServerStartupException("Initializing ConsumerFilterManager failed", e);
        }

        // meta服务端命令处理器
        final BrokerCommandProcessor next = new BrokerCommandProcessor(
                this.storeManager, this.executorsManager, this.statsManager,
                this.remotingServer, metaConfig, this.idWorker, this.brokerZooKeeper, this.consumerFilterManager);

        // 事务命令处理器
        this.brokerProcessor = new TransactionalCommandProcessor(
                metaConfig, this.storeManager, this.idWorker, next, transactionStore, this.statsManager);

        MetaMBeanServer.registMBean(this, null);
    }

    /** 启动MetaQ服务 */
    public synchronized void start() {
        if (!this.shutdown) {
            return;
        }
        this.shutdown = false;

        // 初始化消息存储管理器
        this.storeManager.init();
        //
        this.executorsManager.init();
        // 统计管理器
        this.statsManager.init();
        // 注册命令处理器
        this.registerProcessors();

        try {
            this.remotingServer.start();
        } catch (final NotifyRemotingException e) {
            throw new MetamorphosisServerStartupException("start remoting server failed", e);
        }

        // 将当前broker相关信息注册到zk
        try {
            // 注册broker到zk
            this.brokerZooKeeper.registerBrokerInZk();
            // 给当前broker创建一个 master_config_checksum 节点到zk
            this.brokerZooKeeper.registerMasterConfigFileChecksumInZk();
            // 添加topic变更监听器
            this.addTopicsChangeListener();
            // 注册当前broker的topic信息到zk
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

    /** 停止MetaQ服务 */
    @Override
    public synchronized void stop() {
        if (this.shutdown) {
            return;
        }

        log.info("Stopping metamorphosis server...");
        this.shutdown = true;
        // 将该broker从zk上注销
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

        // 钩子方法执行完成后要从JVM移除
        if (!this.runShutdownHook && this.shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
            }
            catch (Exception e) {
                // ignore
            }
        }

        this.brokerProcessor.dispose();
        // 停止内嵌zk server
        EmbedZookeeperServer.getInstance().stop();

        log.info("Stop metamorphosis server successfully");
    }

    /**
     * 根据配置创建一个删除消息文件的策略实例
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
     * 根据配置信息创建一个用于通讯服务的{@link RemotingServer}对象
     * @param metaConfig MQ配置对象
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
     * 注册命令处理器，客户端会想MQ发送各种请求，包括：GetCommand、PutCommand、OffsetCommand、HeartBeatRequestCommand、QuitCommand、StatsCommand、TransactionCommand
     * 每种类型的请求都有对应的处理器来处理
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
     * 添加topic变更监听器，当当前的MQ服务器的topic变更时，同步到zk上
     */
    private void addTopicsChangeListener() {
        // 监听topics列表变化并注册到zk
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
     * 注册当前broker的topic信息到zk
     * @throws Exception
     */
    private void registerTopicsInZk() throws Exception {
        // 1、先注册配置中的topic到zookeeper
        for (final String topic : this.metaConfig.getTopics()) {
            this.brokerZooKeeper.registerTopicInZk(topic, true);
        }
        // 2、注册消息管理器中的topic到zookeeper
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