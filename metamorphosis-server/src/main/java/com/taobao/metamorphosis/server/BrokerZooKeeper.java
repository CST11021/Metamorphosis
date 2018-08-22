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
package com.taobao.metamorphosis.server;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.json.TopicBroker;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.Utils;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * Broker与ZK交互，注册（或注销）broker和topic等信息到zk
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-24
 * 
 */
public class BrokerZooKeeper implements PropertyChangeListener {

    static final Log log = LogFactory.getLog(BrokerZooKeeper.class);

    /** 表示当前的MQ服务器 */
    private Broker broker = null;
    /** 服务器集群中唯一的id，必须为整型0-1024之间。对服务器集群的定义是使用同一个zookeeper并且在zookeeper上的root path相同，具体参见zookeeper配置 */
    private String brokerIdPath;
    /** MQ服务端配置 */
    private final MetaConfig config;
    /** zk配置 */
    private ZKConfig zkConfig;
    /** zk客户端 */
    private ZkClient zkClient;
    /** 当前broker在zk上的 master_config_checksum 节点（该节点应该是用于判断broker节点的配置有没有发生变化吧）路径，参见{@link MetaZookeeper#masterConfigChecksum(int)}方法*/
    private final String masterConfigChecksumPath;
    /** 表示此broker上发布的topic */
    private final Set<String> topics = new ConcurrentHashSet<String>();
    /** 此broker上的topic及对应topic配置 */
    private final ConcurrentHashMap<String, TopicConfig> cloneTopicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    /** 表示broker注册到zk上时是否失败 */
    private volatile boolean registerBrokerInZkFail = false;
    /** Meta与zookeeper交互的辅助类 */
    private MetaZookeeper metaZookeeper;



    /**
     * 构造器
     * @param metaConfig
     */
    public BrokerZooKeeper(final MetaConfig metaConfig) {
        this.config = metaConfig;
        this.zkConfig = metaConfig.getZkConfig();
        if (this.zkConfig == null) {
            this.zkConfig = this.loadZkConfigFromDiamond();
        }
        // 初始化zk客户端
        this.start(this.zkConfig);
        // 重新计算brokerIdPath
        this.resetBrokerIdPath();
        this.masterConfigChecksumPath = this.metaZookeeper.masterConfigChecksum(this.config.getBrokerId());
        // 监听configFileChecksum属性有没有发送变化
        this.config.addPropertyChangeListener("configFileChecksum", this);
    }

    /**
     * 当有配置参数发生变更时，则同步到zookeeper
     * @param evt
     */
    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        if (evt.getPropertyName().equals("configFileChecksum") && !this.config.isSlave()) {
            try {
                ZkUtils.updateEphemeralPath(
                        this.zkClient, this.masterConfigChecksumPath, String.valueOf(this.config.getConfigFileChecksum()));
            }
            catch (Exception e) {
                log.error("Update master config file checksum to zk failed", e);
            }
        }
    }

    /**
     * 暂时从zk.properties里加载.为了方便单元测试
     * 
     * @return
     */
    private ZKConfig loadZkConfigFromDiamond() {
        Properties properties;
        try {
            properties = Utils.getResourceAsProperties("server.ini", "GBK");
            final ZKConfig zkConfig = new ZKConfig();
            if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnect"))) {
                zkConfig.zkConnect = properties.getProperty("zk.zkConnect");
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkSessionTimeoutMs"))) {
                zkConfig.zkSessionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkSessionTimeoutMs"));
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkConnectionTimeoutMs"))) {
                zkConfig.zkConnectionTimeoutMs = Integer.parseInt(properties.getProperty("zk.zkConnectionTimeoutMs"));
            }

            if (StringUtils.isNotBlank(properties.getProperty("zk.zkSyncTimeMs"))) {
                zkConfig.zkSyncTimeMs = Integer.parseInt(properties.getProperty("zk.zkSyncTimeMs"));
            }

            return zkConfig;
        }
        catch (final IOException e) {
            log.error("zk配置失败", e);
            return null;
        }
        // 尝试从diamond获取
        // this.diamondManager =
        // new DefaultDiamondManager(this.config.getDiamondZKGroup(),
        // this.config.getDiamondZKDataId(),
        // new ManagerListener() {
        // @Override
        // public void receiveConfigInfo(final String configInfo) {
        // log.info("Receiving new diamond zk config:" + configInfo);
        // log.info("Closing zk client");
        // try {
        // BrokerZooKeeper.this.unregisterBrokerInZk();
        // BrokerZooKeeper.this.unregisterTopics();
        // BrokerZooKeeper.this.zkClient.close();
        // final Properties properties = new Properties();
        // properties.load(new StringReader(configInfo));
        // final ZKConfig zkConfig = DiamondUtils.getZkConfig(properties);
        // Thread.sleep(zkConfig.zkSyncTimeMs);
        // BrokerZooKeeper.this.start(zkConfig);
        // BrokerZooKeeper.this.reRegisterEveryThing();
        // log.info("Process new diamond zk config successfully");
        // }
        // catch (final Exception e) {
        // log.error("从diamond加载zk配置失败", e);
        // }
        //
        // }
        //
        //
        // @Override
        // public Executor getExecutor() {
        // return null;
        // }
        // });
        // return null;// DiamondUtils.getZkConfig(this.diamondManager, 10000);
    }

    /**
     * 根据zk配置初始化zk客户端
     * @param zkConfig
     */
    private void start(final ZKConfig zkConfig) {
        log.info("Initialize zk client...");
        this.zkClient = new ZkClient(
                zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer()
        );
        // 订阅session监听
        this.zkClient.subscribeStateChanges(new SessionExpireListener());
        this.metaZookeeper = new MetaZookeeper(this.zkClient, zkConfig.zkRoot);
    }

    /**
     * 重新计算brokerIdPath
     */
    public void resetBrokerIdPath() {
        this.brokerIdPath = this.metaZookeeper.brokerIdsPathOf(this.config.getBrokerId(), this.config.getSlaveId());
    }

    /**
     * 注册broker到zk
     * 
     * @throws Exception
     */
    public void registerBrokerInZk() throws Exception {
        if (!this.zkConfig.zkEnable) {
            return;
        }

        try {
            log.info("Registering broker " + this.brokerIdPath);
            final Broker broker = this.getBroker();

            // 创建临时节点
            ZkUtils.createEphemeralPath(this.zkClient, this.brokerIdPath, broker.getZKString());

            // 兼容老客户端，暂时加上
            if (!this.config.isSlave()) {
                ZkUtils.updateEphemeralPath(
                        this.zkClient,
                        this.metaZookeeper.brokerIdsPath + "/" + this.config.getBrokerId(),
                        broker.getZKString());
                log.info("register for old client version " + this.metaZookeeper.brokerIdsPath + "/"
                        + this.config.getBrokerId() + "  succeeded with " + broker);
            }
            log.info("Registering broker " + this.brokerIdPath + " succeeded with " + broker);
            this.registerBrokerInZkFail = false;
        }
        catch (final Exception e) {
            this.registerBrokerInZkFail = true;
            log.error("注册broker失败");
            throw e;
        }
    }

    /**
     * 根据配置创建一个{@link Broker}对象
     * @return
     * @throws Exception
     */
    public Broker getBroker() throws Exception {
        if (this.broker != null) {
            return this.broker;
        }
        else {
            final String hostName = this.getBrokerHostName();
            this.broker = new Broker(this.config.getBrokerId(), hostName, this.config.getServerPort(), this.config.getSlaveId());
            return this.broker;
        }
    }

    /**
     * 返回broker描述信息
     * @return
     */
    public String getBrokerString() {
        if (this.broker != null) {
            return this.broker.toString();
        }
        else {
            try {
                return this.getBroker().toString();
            }
            catch (Exception e) {
                return null;
            }
        }
    }

    /**
     * 获取MQ服务器所在的机器(IP),从 {@link MetaConfig#hostName} 获取，如果没有配置，默认为本地
     * @return
     * @throws Exception
     */
    public String getBrokerHostName() throws Exception {
        final String hostName =  this.config.getHostName() == null ? RemotingUtils.getLocalHost() : this.config.getHostName();
        return hostName;
    }

    /**
     * 给当前broker创建一个 master_config_checksum 节点
     * @throws Exception
     */
    public void registerMasterConfigFileChecksumInZk() throws Exception {
        if (!this.zkConfig.zkEnable) {
            return;
        }

        try {
            if (!this.config.isSlave()) {
                ZkUtils.createEphemeralPath(
                        this.zkClient, this.masterConfigChecksumPath, String.valueOf(this.config.getConfigFileChecksum()));
            }
        }
        catch (final Exception e) {
            this.registerBrokerInZkFail = true;
            log.error("注册broker失败");
            throw e;
        }
    }

    /**
     * 将broker从zk上注销
     * @throws Exception
     */
    private void unregisterBrokerInZk() throws Exception {
        if (this.registerBrokerInZkFail) {
            log.warn("上次注册broker未成功,不需要unregister");
            return;
        }
        ZkUtils.deletePath(this.zkClient, this.brokerIdPath);
        if (!this.config.isSlave()) {
            ZkUtils.deletePath(this.zkClient, this.masterConfigChecksumPath);
        }
        // 兼容老客户端，暂时加上.
        if (!this.config.isSlave()) {
            try {
                ZkUtils.deletePath(this.zkClient, this.metaZookeeper.brokerIdsPath + "/" + this.config.getBrokerId());
            }
            catch (final Exception e) {
                // 有slave时是删不掉的,写个空值进去
                ZkUtils.updateEphemeralPath(this.zkClient,
                    this.metaZookeeper.brokerIdsPath + "/" + this.config.getBrokerId(), "");
            }
            log.info("delete broker of old client version " + this.metaZookeeper.brokerIdsPath + "/"
                    + this.config.getBrokerId());
        }
    }

    /**
     * 将topic从zk上注销
     * @throws Exception
     */
    private void unregisterTopics() throws Exception {
        for (final String topic : BrokerZooKeeper.this.topics) {
            this.unregisterTopic(topic);
        }
    }

    /**
     * 将broker和topics从zk上注销
     */
    public void unregisterEveryThing() {
        try {
            this.unregisterBrokerInZk();
            this.unregisterTopics();
        }
        catch (Exception e) {
            log.error("Unregister broker failed", e);
        }
    }

    /**
     * 将topic从zk上注销
     * @param topic
     */
    private void unregisterTopic(final String topic) {
        try {
            int brokerId = this.config.getBrokerId();
            // 获取brokerTopic的存储路径
            final String brokerTopicPath = this.metaZookeeper.brokerTopicsPathOf(topic, brokerId, this.config.getSlaveId());
            //
            final String topicPubPath = this.metaZookeeper.brokerTopicsPathOf(topic, true, brokerId, this.config.getSlaveId());
            //
            final String topicSubPath = this.metaZookeeper.brokerTopicsPathOf(topic, false, brokerId, this.config.getSlaveId());

            // Be compatible with the version before 1.4.3
            ZkUtils.deletePath(this.zkClient, brokerTopicPath);

            // added by dennis,since 1.4.3
            ZkUtils.deletePath(this.zkClient, topicPubPath);

            ZkUtils.deletePath(this.zkClient, topicSubPath);
        }
        catch (Exception e) {
            log.error("Unregister topic " + topic + " failed,but don't worry about it.", e);
        }
    }

    /**
     * 注册topic和分区信息到zk
     * 
     * @param topic
     * @param force
     *
     * @throws Exception
     */
    public void registerTopicInZk(final String topic, boolean force) throws Exception {
        if (force) {
            // This block is not synchronized,because we don't force to register
            // topics frequently except reloading config file.
            TopicConfig oldConfig = this.cloneTopicConfigs.get(topic);
            TopicConfig newConfig = this.config.getTopicConfig(topic);
            if (this.compareTopicConfigs(newConfig, oldConfig)) {
                return;
            }
            else {
                this.unregisterTopic(topic);
                this.topics.add(topic);
                this.registerTopicInZkInternal(topic);
            }
        }
        else {
            if (!this.topics.add(topic)) {
                return;
            }
            else {
                this.registerTopicInZkInternal(topic);
            }
        }
    }

    /**
     * 比较两个配置是否一样
     * @param c1
     * @param c2
     * @return
     */
    private boolean compareTopicConfigs(TopicConfig c1, TopicConfig c2) {
        if (c1 == null && c2 != null) {
            return false;
        }
        if (c2 == null && c1 != null) {
            return false;
        }
        // If old and new configs are all null,we have to register it.
        if (c1 == null && c2 == null) {
            return false;
        }
        return c1.equals(c2);
    }

    /**
     * 将broker和topic发布（注册）到zk
     * @throws Exception
     */
    public void reRegisterEveryThing() throws Exception {
        log.info("re-registering broker info in ZK for broker " + BrokerZooKeeper.this.config.getBrokerId());
        BrokerZooKeeper.this.registerBrokerInZk();
        log.info("re-registering broker topics in ZK for broker " + BrokerZooKeeper.this.config.getBrokerId());
        for (final String topic : BrokerZooKeeper.this.topics) {
            BrokerZooKeeper.this.registerTopicInZkInternal(topic);
        }
        this.registerMasterConfigFileChecksumInZk();
        log.info("done re-registering broker");
    }

    /**
     * 发布topic到zk
     * @param topic
     * @throws Exception
     */
    private void registerTopicInZkInternal(final String topic) throws Exception {
        if (!this.zkConfig.zkEnable) {
            log.warn("zkEnable is false,so we don't talk to zookeeper.");
            return;
        }
        int brokerId = this.config.getBrokerId();
        int slaveId = this.config.getSlaveId();
        final String brokerTopicPath = this.metaZookeeper.brokerTopicsPathOf(topic, brokerId, slaveId);
        final String topicPubPath = this.metaZookeeper.brokerTopicsPathOf(topic, true, brokerId, slaveId);
        final String topicSubPath = this.metaZookeeper.brokerTopicsPathOf(topic, false, brokerId, slaveId);
        final TopicConfig topicConfig = this.config.getTopicConfig(topic);
        Integer numParts = topicConfig != null ? topicConfig.getNumPartitions() : this.config.getNumPartitions();
        numParts = numParts == null ? this.config.getNumPartitions() : numParts;
        log.info("Begin registering broker topic " + brokerTopicPath + " with " + numParts + " partitions");

        final TopicBroker topicBroker = new TopicBroker(numParts, brokerId + (slaveId >= 0 ? "-s" + slaveId : "-m"));
        log.info("Register broker for topic:" + topicBroker);
        // Be compatible with the version before 1.4.3
        ZkUtils.createEphemeralPath(this.zkClient, brokerTopicPath, String.valueOf(numParts));

        // added by dennis,since 1.4.3
        String topicBrokerJson = topicBroker.toJson();
        if (topicConfig.isAcceptPublish()) {
            ZkUtils.createEphemeralPath(this.zkClient, topicPubPath, topicBrokerJson);
        }
        else {
            ZkUtils.deletePath(this.zkClient, topicPubPath);
        }

        if (topicConfig.isAcceptSubscribe()) {
            ZkUtils.createEphemeralPath(this.zkClient, topicSubPath, topicBrokerJson);
        }
        else {
            ZkUtils.deletePath(this.zkClient, topicSubPath);
        }
        this.cloneTopicConfigs.put(topic, topicConfig.clone());

        log.info("End registering broker topic " + brokerTopicPath);
    }

    /**
     * 是否从zk注销该broker
     * @param unregister
     */
    public void close(boolean unregister) {
        try {
            if (unregister && this.zkConfig.zkEnable) {
                this.unregisterBrokerInZk();
                this.unregisterTopics();
            }
        }
        catch (final Exception e) {
            log.warn("error on unregisterBrokerInZk", e);
        }
        finally {
            if (this.zkClient != null) {
                log.info("Closing zk client...");
                this.zkClient.close();
            }
        }
    }

    /**
     * session过期监听器
     */
    private class SessionExpireListener implements IZkStateListener {

        @Override
        public void handleNewSession() throws Exception {
            BrokerZooKeeper.this.reRegisterEveryThing();
        }

        @Override
        public void handleStateChanged(final KeeperState state) throws Exception {
            // do nothing, since zkclient will do reconnect for us.
        }

    }

    MetaConfig getConfig() {
        return this.config;
    }
    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }
    public Set<String> getTopics() {
        return this.topics;
    }
    public ZkClient getZkClient() {
        return this.zkClient;
    }
    public MetaZookeeper getMetaZookeeper() {
        return this.metaZookeeper;
    }

}