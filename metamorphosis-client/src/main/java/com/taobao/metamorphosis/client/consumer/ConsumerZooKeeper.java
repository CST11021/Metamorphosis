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

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.ZkClientChangedListener;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Cluster;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.MetaZookeeper.ZKGroupDirs;
import com.taobao.metamorphosis.utils.MetaZookeeper.ZKGroupTopicDirs;
import com.taobao.metamorphosis.utils.ThreadUtils;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * Consumer与Zookeeper交互：将消费者注册到zk上，或从zk上注销消费者
 * 
 * @author boyan
 * @Date 2011-4-26
 * @author wuhua
 * @Date 2011-6-26
 */
public class ConsumerZooKeeper implements ZkClientChangedListener {

    static final Log log = LogFactory.getLog(ConsumerZooKeeper.class);

    static final int MAX_N_RETRIES = 7;

    /** ZK客户端，用于与zk交互 */
    protected ZkClient zkClient;

    /** zk配置 */
    private final ZKConfig zkConfig;

    /** 通讯客户端，用于MQ服务器通讯 */
    private final RemotingClientWrapper remotingClient;

    /** 用于获取broken注册在zk上的信息 */
    protected final MetaZookeeper metaZookeeper;

    /** 保存消息抓取器对应的ZKLoadRebalanceListener */
    protected final ConcurrentHashMap<FetchManager, FutureTask<ZKLoadRebalanceListener>> consumerLoadBalanceListeners = new ConcurrentHashMap<FetchManager, FutureTask<ZKLoadRebalanceListener>>();

    public ConsumerZooKeeper(final MetaZookeeper metaZookeeper, final RemotingClientWrapper remotingClient, final ZkClient zkClient, final ZKConfig zkConfig) {
        super();
        this.metaZookeeper = metaZookeeper;
        this.zkClient = zkClient;
        this.remotingClient = remotingClient;
        this.zkConfig = zkConfig;
    }

    /**
     * 当新的zkClient建立的时候，调用该方法
     *
     * @param newClient
     */
    @Override
    public void onZkClientChanged(final ZkClient newClient) {
        this.zkClient = newClient;
        // 重新注册consumer
        for (final FutureTask<ZKLoadRebalanceListener> task : this.consumerLoadBalanceListeners.values()) {
            try {
                final ZKLoadRebalanceListener listener = task.get();
                // 要清空已有的注册信息，防止在注册consumer失败的时候还提交offset，导致覆盖更新的offset
                listener.topicRegistry.clear();
                log.info("re-register consumer to zk,group=" + listener.consumerConfig.getGroup());
                this.registerConsumerInternal(listener);
            } catch (final Exception e) {
                log.error("reRegister consumer failed", e);
            }
        }

    }

    /**
     * 将当前消息抓取器抓取的消息偏移量保存到zk
     *
     * @param fetchManager
     */
    public void commitOffsets(final FetchManager fetchManager) {
        final ZKLoadRebalanceListener listener = this.getBrokerConnectionListener(fetchManager);
        if (listener != null) {
            listener.commitOffsets();
        }
    }

    /**
     * 注销consumer
     * 
     * @param fetchManager
     */
    public void unRegisterConsumer(final FetchManager fetchManager) {
        try {
            final FutureTask<ZKLoadRebalanceListener> futureTask =
                    this.consumerLoadBalanceListeners.remove(fetchManager);
            if (futureTask != null) {
                final ZKLoadRebalanceListener listener = futureTask.get();
                if (listener != null) {
                    listener.stop();
                    // 提交offsets
                    listener.commitOffsets();
                    this.zkClient.unsubscribeStateChanges(new ZKSessionExpireListenner(listener));
                    final ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(listener.consumerConfig.getGroup());
                    this.zkClient.unsubscribeChildChanges(dirs.consumerRegistryDir, listener);
                    log.info("unsubscribeChildChanges:" + dirs.consumerRegistryDir);
                    // 移除监视订阅topic的分区变化
                    for (final String topic : listener.topicSubcriberRegistry.keySet()) {
                        final String partitionPath = this.metaZookeeper.brokerTopicsSubPath + "/" + topic;
                        this.zkClient.unsubscribeChildChanges(partitionPath, listener);
                        log.info("unsubscribeChildChanges:" + partitionPath);
                    }
                    // 删除ownership
                    listener.releaseAllPartitionOwnership();
                    // 删除临时节点
                    ZkUtils.deletePath(this.zkClient, listener.dirs.consumerRegistryDir + "/"
                            + listener.consumerIdString);

                }
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted when unRegisterConsumer", e);
        }
        catch (final Exception e) {
            log.error("Error in unRegisterConsumer,maybe error when registerConsumer", e);
        }
    }

    /**
     * 向zk注册消息消费者
     *
     * @param consumerConfig            消费者配置信息
     * @param fetchManager              消息抓取器
     * @param topicSubcriberRegistry    topic订阅注册中心
     * @param offsetStorage             消息抓取的初始索引
     * @param loadBalanceStrategy       消费者的负载均衡策略
     * @throws Exception
     */
    public void registerConsumer(final ConsumerConfig consumerConfig, final FetchManager fetchManager, final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry, final OffsetStorage offsetStorage, final LoadBalanceStrategy loadBalanceStrategy) throws Exception {

        final FutureTask<ZKLoadRebalanceListener> task = new FutureTask<ZKLoadRebalanceListener>(
                new Callable<ZKLoadRebalanceListener>() {
                    @Override
                    public ZKLoadRebalanceListener call() throws Exception {
                        // 消费者在zk上的路径信息
                        final ZKGroupDirs dirs = ConsumerZooKeeper.this.metaZookeeper.new ZKGroupDirs(consumerConfig.getGroup());
                        // 获取消费者实例的id，如果有配置消费者的id，则使用使用配置的id，否则使用uuid生成器
                        final String consumerUUID = ConsumerZooKeeper.this.getConsumerUUID(consumerConfig);
                        final String consumerUUIDString = consumerConfig.getGroup() + "_" + consumerUUID;

                        //
                        final ZKLoadRebalanceListener loadBalanceListener = new ZKLoadRebalanceListener(fetchManager, dirs, consumerUUIDString, consumerConfig, offsetStorage, topicSubcriberRegistry, loadBalanceStrategy);
                        loadBalanceListener.start();
                        // 注册消费者，并启用异步线程开始发起抓取消息的请求
                        return ConsumerZooKeeper.this.registerConsumerInternal(loadBalanceListener);
                    }
                });

        // putIfAbsent方法：
        //      如果传入key对应的value已经存在，就返回存在的value，不进行替换；
        //      如果不存在，就添加key和value，返回null。
        final FutureTask<ZKLoadRebalanceListener> existsTask = this.consumerLoadBalanceListeners.putIfAbsent(fetchManager, task);

        if (existsTask == null) {
            task.run();
        } else {
            throw new MetaClientException("Consumer has been already registed");
        }

    }

    public ZKLoadRebalanceListener getBrokerConnectionListener(final FetchManager fetchManager) {
        final FutureTask<ZKLoadRebalanceListener> task = this.consumerLoadBalanceListeners.get(fetchManager);
        if (task != null) {
            try {
                return task.get();
            }
            catch (final ExecutionException e) {
                throw ThreadUtils.launderThrowable(e.getCause());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }





    /**
     * 注册消费者，并开始发起抓取消息的请求
     *
     * @param loadBalanceListener
     * @return
     * @throws UnknownHostException
     * @throws InterruptedException
     * @throws Exception
     */
    protected ZKLoadRebalanceListener registerConsumerInternal(final ZKLoadRebalanceListener loadBalanceListener) throws UnknownHostException, InterruptedException, Exception {
        final ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(loadBalanceListener.consumerConfig.getGroup());
        // 获取所有被订阅的topic，用逗号分隔
        final String topicString = this.getTopicsString(loadBalanceListener.topicSubcriberRegistry);

        // 直连MQ服务的模式
        if (this.zkClient == null) {
            loadBalanceListener.fetchManager.stopFetchRunner();
            loadBalanceListener.fetchManager.resetFetchState();

            // zkClient为null，使用配置项并发起fetch请求
            for (final String topic : loadBalanceListener.topicSubcriberRegistry.keySet()) {
                final SubscriberInfo subInfo = loadBalanceListener.topicSubcriberRegistry.get(topic);
                ConcurrentHashMap<Partition, TopicPartitionRegInfo> topicPartRegInfoMap = loadBalanceListener.topicRegistry.get(topic);
                if (topicPartRegInfoMap == null) {
                    topicPartRegInfoMap = new ConcurrentHashMap<Partition, TopicPartitionRegInfo>();
                    loadBalanceListener.topicRegistry.put(topic, topicPartRegInfoMap);
                }


                final Partition partition = new Partition(loadBalanceListener.consumerConfig.getPartition());
                long offset = loadBalanceListener.consumerConfig.getOffset();

                // 判断每次订阅是否从最新位置开始消费,如果为true，表示每次启动都从最新位置开始消费,通常在测试的时候可以设置为true
                if (loadBalanceListener.consumerConfig.isAlwaysConsumeFromMaxOffset()) {
                    offset = Long.MAX_VALUE;
                }

                final TopicPartitionRegInfo regInfo = new TopicPartitionRegInfo(topic, partition, offset);
                topicPartRegInfoMap.put(partition, regInfo);
                // 创建一个消息抓取的请求，并添加到本地队列中
                loadBalanceListener.fetchManager.addFetchRequest(
                        new FetchRequest(
                                new Broker(0, loadBalanceListener.consumerConfig.getServerUrl()),
                                0L,
                                regInfo,
                                subInfo.getMaxSize()
                        )
                );
            }
            loadBalanceListener.fetchManager.startFetchRunner();
        }
        // 常规模式：通过zk服务发现
        else {
            for (int i = 0; i < MAX_N_RETRIES; i++) {
                // 在zk上创建保存consumer id的目录
                ZkUtils.makeSurePersistentPathExists(this.zkClient, dirs.consumerRegistryDir);
                // 创建一个数据节点：/meta/consumers/${分组名}/ids/${消费者的id标识}
                ZkUtils.createEphemeralPathExpectConflict(this.zkClient, dirs.consumerRegistryDir + "/" + loadBalanceListener.consumerIdString, topicString);

                // 监视同一个分组的consumer列表是否有变化
                // 监听"/meta/consumers/${分组名}/ids/"目录的节点变更情况
                this.zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalanceListener);

                // 监视订阅topic的分区是否有变化
                for (final String topic : loadBalanceListener.topicSubcriberRegistry.keySet()) {
                    final String partitionPath = this.metaZookeeper.brokerTopicsSubPath + "/" + topic;
                    // 保存分区路径到zk
                    ZkUtils.makeSurePersistentPathExists(this.zkClient, partitionPath);
                    this.zkClient.subscribeChildChanges(partitionPath, loadBalanceListener);
                }

                // 监视zk client状态，在连接重连的时候重新注册
                this.zkClient.subscribeStateChanges(new ZKSessionExpireListenner(loadBalanceListener));

                // 第一次，需要明确触发balance
                if (loadBalanceListener.syncedRebalance()) {
                    break;
                }
            }
        }
        return loadBalanceListener;
    }
    /**
     * 返回所有被订阅的topic，多个topic之间用逗号分隔
     * @param topicSubcriberRegistry
     * @return
     */
    private String getTopicsString(final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry) {
        final StringBuilder topicSb = new StringBuilder();
        boolean wasFirst = true;
        for (final String topic : topicSubcriberRegistry.keySet()) {
            if (wasFirst) {
                wasFirst = false;
                topicSb.append(topic);
            }
            else {
                topicSb.append(",").append(topic);
            }
        }
        return topicSb.toString();
    }
    /**
     * 返回消费的id，如果有配置消费者的id，则使用使用配置的id，否则使用uuid生成器
     *
     * @param consumerConfig
     * @return
     * @throws Exception
     */
    protected String getConsumerUUID(final ConsumerConfig consumerConfig) throws Exception {
        String consumerUUID = null;
        if (consumerConfig.getConsumerId() != null) {
            consumerUUID = consumerConfig.getConsumerId();
        }
        else {
          consumerUUID = RemotingUtils.getLocalHost() + "-" + this.getPid() + "-" + UUID.randomUUID();
        }
        return consumerUUID;
    }
    private String getPid() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name.contains("@")) {
            return name.split("@")[0];
        }
        return name;
    }



    /**
     * 当zookeeper客户端状态变更时监听器
     */
    class ZKSessionExpireListenner implements IZkStateListener {
        private final String consumerIdString;
        private final ZKLoadRebalanceListener loadBalancerListener;


        public ZKSessionExpireListenner(final ZKLoadRebalanceListener loadBalancerListener) {
            super();
            this.consumerIdString = loadBalancerListener.consumerIdString;
            this.loadBalancerListener = loadBalancerListener;
        }

        @Override
        public void handleNewSession() throws Exception {
            /**
             * When we get a SessionExpired event, we lost all ephemeral nodes
             * and zkclient has reestablished a connection for us. We need to
             * release the ownership of the current consumer and re-register
             * this consumer in the consumer registry and trigger a rebalance.
             */
            ;
            log.info("ZK expired; release old broker parition ownership; re-register consumer " + this.consumerIdString);
            this.loadBalancerListener.resetState();
            ConsumerZooKeeper.this.registerConsumerInternal(this.loadBalancerListener);
        }

        @Override
        public void handleStateChanged(final KeeperState state) throws Exception {
            // do nothing, since zkclient will do reconnect for us.

        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof ZKSessionExpireListenner)) {
                return false;
            }
            final ZKSessionExpireListenner other = (ZKSessionExpireListenner) obj;
            return this.loadBalancerListener.equals(other.loadBalancerListener);
        }

        @Override
        public int hashCode() {
            return this.loadBalancerListener.hashCode();
        }

    }

    /**
     * zookeeper的消费者负载平衡监听器，当消费者变更时，会重新分配哪些消费者消费对应topic下的分区消息
     * This is a internal class for consumer,you should not use it directly in your code.
     *
     * zk客户端通过 zkClient.subscribeChildChanges 方法来注册IZkChildListener事件
     * 
     * @author dennis<killme2008@gmail.com>
     * 
     */
    public class ZKLoadRebalanceListener implements IZkChildListener, Runnable {

        private final Thread rebalanceThread;

        /** 消费者在zk上的注册的信息 */
        private final ZKGroupDirs dirs;
        /** 表示消费者所在分组信息 */
        private final String group;
        /** 消费者的分组 + consumerUUID */
        protected final String consumerIdString;
        /** 消息消费者的负载均衡策略 */
        private final LoadBalanceStrategy loadBalanceStrategy;
        /** 表示当前topic对应的订阅者信息：Map<topic, topic的订阅者> */
        Map<String, List<String>> oldConsumersPerTopicMap = new HashMap<String, List<String>>();
        /** 表示当前topic对应的分区信息：Map<topic, topic的分区索引> */
        Map<String, List<String>> oldPartitionsPerTopicMap = new HashMap<String, List<String>>();
        /** 重均衡锁：当topic对应的订阅者或分区变更时，MateQ会重新均衡topic及对应的订阅者，重新均衡通过该锁来实现同步 */
        private final Lock rebalanceLock = new ReentrantLock();
        /** 订阅的topic对应的broker,offset等信息, Map<topic, Map<Partition, TopicPartitionRegInfo>> */
        final ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry = new ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>>();
        /** topic的订阅信息，topic对应的监听器等，Map<topic, SubscriberInfo> */
        private final ConcurrentHashMap<String, SubscriberInfo> topicSubcriberRegistry;
        /** 消费者配置信息 */
        private final ConsumerConfig consumerConfig;
        /** 消费者抓取消息时初始索引（消息偏移量） */
        private final OffsetStorage offsetStorage;
        /** 消息抓取管理器 */
        private final FetchManager fetchManager;

        private volatile boolean stopped = false;
        /** 表示当前的broker */
        Set<Broker> oldBrokerSet = new HashSet<Broker>();
        /** 表示当前的MQ集群 */
        private Cluster oldCluster = new Cluster();
        /** 当消费者变更时（zk上消费者信息改变时），在往该队列添加一个{@link #REBALANCE_EVT}字节，代表消费者发生过一次变更 */
        private final BlockingQueue<Byte> rebalanceEvents = new ArrayBlockingQueue<Byte>(10);
        /** 表示消费者的变更事件 */
        private final Byte REBALANCE_EVT = (byte) 1;


        public ZKLoadRebalanceListener(final FetchManager fetchManager,
                                       final ZKGroupDirs dirs,
                                       final String consumerIdString,
                                       final ConsumerConfig consumerConfig,
                                       final OffsetStorage offsetStorage,
                                       final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry,
                                       final LoadBalanceStrategy loadBalanceStrategy) {
            super();
            this.fetchManager = fetchManager;
            this.dirs = dirs;
            this.consumerIdString = consumerIdString;
            this.group = consumerConfig.getGroup();
            this.consumerConfig = consumerConfig;
            this.offsetStorage = offsetStorage;
            this.topicSubcriberRegistry = topicSubcriberRegistry;
            this.loadBalanceStrategy = loadBalanceStrategy;
            this.rebalanceThread = new Thread(this);
        }

        public void start() {
            // start后，即执行run()方法
            this.rebalanceThread.start();
        }

        /**
         * 1、当消费者被创建，并注册到zk后，会调用该线程方法；
         * 2、消费者被销毁后，也会调用该方法
         */
        @Override
        public void run() {
            while (!this.stopped) {
                try {
                    Byte evt = this.rebalanceEvents.take();
                    // 不为空说明zk上注册的消费者有变更
                    if (evt != null) {
                        // 移除所有的变更事件
                        this.dropDuplicatedEvents();

                        this.syncedRebalance();
                    }
                } catch (InterruptedException e) {
                    // continue;
                } catch (Throwable e) {
                    log.error("Rebalance failed.", e);
                }
            }

        }

        /**
         * 当前子节点改变时，触发该监听方法
         *
         * @param parentPath        父节点路径，这里的父节点路径包含：
         *                          ①/meta/consumers/${分组名}/ids/，该路径下保存多个ConsumerID，表示注册在zk上的consumer
         *                          ②/meta/brokers/topics-sub/，该路径下报错多个topic，表示被订阅的topic
         *
         * @param currentChilds     子节点列表，这里可能是：消费者的id标识 或 被订阅的topic
         * @throws Exception
         */
        @Override
        public void handleChildChange(final String parentPath, final List<String> currentChilds) throws Exception {
            this.rebalanceEvents.put(this.REBALANCE_EVT);
        }

        public void stop() {
            this.stopped = true;
            this.rebalanceThread.interrupt();
            try {
                this.rebalanceThread.join(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * 根据consumerId获取订阅的topic列表
         *
         * @param consumerId
         * @return Map<topic, consumerId>
         * @throws Exception
         */
        public Map<String, String> getConsumerPerTopic(final String consumerId) throws Exception {
            final List<String> topics = this.getTopics(consumerId);
            final Map<String/* topic */, String/* consumerId */> rt = new HashMap<String, String>();
            for (final String topic : topics) {
                rt.put(topic, consumerId);
            }
            return rt;
        }

        /**
         * Returns current topic-partitions info.
         *
         * @since 1.4.4
         * @return
         */
        public Map<String/* topic */, Set<Partition>> getTopicPartitions() {
            Map<String, Set<Partition>> rt = new HashMap<String, Set<Partition>>();
            for (Map.Entry<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry.entrySet()) {
                rt.put(entry.getKey(), entry.getValue().keySet());
            }
            return rt;
        }

        /**
         * 更新offset到zk
         */
        private void commitOffsets() {
            this.offsetStorage.commitOffset(this.consumerConfig.getGroup(), this.getTopicPartitionRegInfos());
        }

        private TopicPartitionRegInfo initTopicPartitionRegInfo(final String topic, final String group, final Partition partition, final long offset) {
            this.offsetStorage.initOffset(topic, group, partition, offset);
            return new TopicPartitionRegInfo(topic, partition, offset);
        }

        /**
         * 返回所有被订阅的topic的所有分区
         *
         * @return
         */
        List<TopicPartitionRegInfo> getTopicPartitionRegInfos() {
            final List<TopicPartitionRegInfo> rt = new ArrayList<TopicPartitionRegInfo>();
            // 遍历所有被订阅的topic的所有分区
            for (final ConcurrentHashMap<Partition, TopicPartitionRegInfo> subMap : this.topicRegistry.values()) {
                final Collection<TopicPartitionRegInfo> values = subMap.values();
                if (values != null) {
                    rt.addAll(values);
                }
            }
            return rt;
        }

        /**
         * 加载offset信息
         * 
         * @param topic
         * @param partition
         * @return
         */
        private TopicPartitionRegInfo loadTopicPartitionRegInfo(final String topic, final Partition partition) {
            return this.offsetStorage.load(topic, this.consumerConfig.getGroup(), partition);
        }

        /**
         * 删除掉重复的事件，这里是将 {@link #rebalanceEvents} 中所有的事件移除
         */
        private void dropDuplicatedEvents() {
            Byte evt = null;
            int count = 0;
            // poll()：移除并返回头部的元素
            while ((evt = this.rebalanceEvents.poll()) != null) {
                // poll out duplicated events.
                count++;
            }
            if (count > 0) {
                log.info("Drop " + count + " duplicated rebalance events");
            }
        }

        /**
         * 使用同步的方式重新平衡
         *
         * @return
         * @throws InterruptedException
         * @throws Exception
         */
        boolean syncedRebalance() throws InterruptedException, Exception {
            this.rebalanceLock.lock();
            try {
                for (int i = 0; i < MAX_N_RETRIES; i++) {
                    log.info("begin rebalancing consumer " + this.consumerIdString + " try #" + i);
                    boolean done;
                    try {
                        done = this.rebalance();
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (final Throwable e) {
                        // 发生了预料之外的异常,都重试一下,
                        // 有可能是多个机器consumer在同时rebalance造成的读取zk数据不一致,-- wuhua
                        log.warn("unexpected exception occured while try rebalancing", e);
                        done = false;
                    }

                    log.warn("end rebalancing consumer " + this.consumerIdString + " try #" + i);

                    if (done) {
                        log.warn("rebalance success.");
                        return true;
                    } else {
                        log.warn("rebalance failed,try #" + i);
                    }

                    // release all partitions, reset state and retry
                    this.releaseAllPartitionOwnership();
                    this.resetState();
                    // 等待zk数据同步
                    Thread.sleep(ConsumerZooKeeper.this.zkConfig.zkSyncTimeMs);
                }
                log.error("rebalance failed,finally");
                return false;
            }
            finally {
                this.rebalanceLock.unlock();
            }
        }

        private void resetState() {
            this.topicRegistry.clear();
            this.oldConsumersPerTopicMap.clear();
            this.oldPartitionsPerTopicMap.clear();
        }

        /**
         * 更新fetch线程
         * 
         * @param cluster
         */
        protected void updateFetchRunner(final Cluster cluster) throws Exception {
            this.fetchManager.resetFetchState();
            final Set<Broker> newBrokers = new HashSet<Broker>();
            for (final Map.Entry<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry.entrySet()) {
                final String topic = entry.getKey();
                for (final Map.Entry<Partition, TopicPartitionRegInfo> partEntry : entry.getValue().entrySet()) {
                    final Partition partition = partEntry.getKey();
                    final TopicPartitionRegInfo info = partEntry.getValue();
                    // 随机取master或slave的一个读,wuhua
                    final Broker broker = cluster.getBrokerRandom(partition.getBrokerId());
                    if (broker != null) {
                        newBrokers.add(broker);
                        final SubscriberInfo subscriberInfo = this.topicSubcriberRegistry.get(topic);
                        // 添加fetch请求
                        this.fetchManager.addFetchRequest(new FetchRequest(broker, 0L, info, subscriberInfo.getMaxSize()));
                    }
                    else {
                        log.error("Could not find broker for broker id " + partition.getBrokerId() + ", it should not happen.");
                    }
                }
            }

            for (Broker newOne : newBrokers) {
                int times = 0;
                NotifyRemotingException ne = null;
                while (times++ < 3) {
                    // 通信层客户端连接到MQ服务器
                    ConsumerZooKeeper.this.remotingClient.connectWithRef(newOne.getZKString(), this);
                    try {
                        ConsumerZooKeeper.this.remotingClient.awaitReadyInterrupt(newOne.getZKString(), 4000);
                        log.warn("Connected to " + newOne.getZKString());
                        break;
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Remoting client is interrupted", e);
                    }
                    catch (NotifyRemotingException e) {
                        times++;
                        ne = e;
                        continue;
                    }
                }
                if (ne != null) {
                    // Throw it to do rebalancing.
                    throw ne;
                }
            }
            // 重新启动fetch线程
            log.warn("Starting fetch runners");
            this.oldBrokerSet = newBrokers;
            this.fetchManager.startFetchRunner();
        }

        /**
         * 当topic对应的订阅者或分区变更时，MateQ会重新均衡topic及对应的订阅者
         *
         * 重新均衡时需要做如下几步：
         * 1、fetchManager.stopFetchRunner() 消息抓取器停止抓取消息
         * 2、关闭连接到MQ的服务的客户端
         * 3、将当前的offset更新到zk
         * 4、updateFetchRunner() 更新消息抓取理的fetch线程和fetch请求，然后连接到新的broker，最后再启动消息抓取器去抓取消息
         *
         *
         * @return
         * @throws InterruptedException
         * @throws Exception
         */
        boolean rebalance() throws InterruptedException, Exception {
            // 获取这个consumer订阅的topic
            final Map<String/* topic */, String/* consumerId */> myConsumerPerTopicMap = this.getConsumerPerTopic(this.consumerIdString);
            // 获取所有broker集群信息
            final Cluster cluster = ConsumerZooKeeper.this.metaZookeeper.getCluster();

            Map<String/* topic */, List<String>/* consumer list */> consumersPerTopicMap = null;
            try {
                // 获取指定分组订阅的topic到订阅者之间的映射关系
                consumersPerTopicMap = this.getConsumersPerTopic(this.group);
            }
            catch (final NoNodeException e) {
                // 多个consumer同时在负载均衡时,可能会到达这里 -- wuhua
                log.warn("maybe other consumer is rebalancing now," + e.getMessage());
                return false;
            }
            catch (final ZkNoNodeException e) {
                // 多个consumer同时在负载均衡时,可能会到达这里 -- wuhua
                log.warn("maybe other consumer is rebalancing now," + e.getMessage());
                return false;
            }

            // 获取zk上注册的topic到partition映射的map. 包括master和slave的所有partitions
            final Map<String, List<String>> partitionsPerTopicMap = this.getPartitionStringsForTopics(myConsumerPerTopicMap);

            // 获取有变更的topic跟consumer集合
            final Map<String/* topic */, String/* consumer id */> relevantTopicConsumerIdMap = this.getRelevantTopicMap(myConsumerPerTopicMap, partitionsPerTopicMap, this.oldPartitionsPerTopicMap, consumersPerTopicMap, this.oldConsumersPerTopicMap);

            // 没有变更，无需平衡
            if (relevantTopicConsumerIdMap.size() <= 0) {
                // 处理主备情况,topic分区和消费者没有变化,但主备的其中一台挂了,
                // 导致partitionsPerTopicMap可能是没有变化的,
                // 所以要检查集群的变化并重新连接
                if (this.checkClusterChange(cluster)) {
                    log.warn("Stopping fetch runners,maybe master or slave changed");
                    this.fetchManager.stopFetchRunner();
                    // closed all connections to old brokers.
                    this.closeOldBrokersConnections();
                    this.commitOffsets();
                    this.updateFetchRunner(cluster);
                    this.oldCluster = cluster;
                }
                else {
                    log.warn("Consumer " + this.consumerIdString + " with " + consumersPerTopicMap + " doesn't need to be rebalanced.");
                }
                return true;
            }

            log.warn("Stopping fetch runners");
            this.fetchManager.stopFetchRunner();
            // closed all connections to old brokers.
            this.closeOldBrokersConnections();
            log.warn("Comitting all offsets");
            this.commitOffsets();

            for (final Map.Entry<String, String> entry : relevantTopicConsumerIdMap.entrySet()) {
                final String topic = entry.getKey();
                final String consumerId = entry.getValue();

                final ZKGroupTopicDirs topicDirs = ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.group);
                // 当前该topic的订阅者
                final List<String> curConsumers = consumersPerTopicMap.get(topic);
                // 当前该topic的分区
                final List<String> curPartitions = partitionsPerTopicMap.get(topic);

                if (curConsumers == null) {
                    log.warn("Releasing partition ownerships for topic:" + topic);
                    this.releasePartitionOwnership(topic);
                    this.topicRegistry.remove(topic);
                    log.warn("There are no consumers subscribe topic " + topic);
                    continue;
                }
                if (curPartitions == null) {
                    log.warn("Releasing partition ownerships for topic:" + topic);
                    this.releasePartitionOwnership(topic);
                    this.topicRegistry.remove(topic);
                    log.warn("There are no partitions under topic " + topic);
                    continue;
                }

                // 根据负载均衡策略获取这个consumer对应的partition列表
                final List<String> newParts = this.loadBalanceStrategy.getPartitions(topic, consumerId, curConsumers, curPartitions);

                // 查看当前这个topic的分区列表，查看是否有变更
                ConcurrentHashMap<Partition, TopicPartitionRegInfo> partRegInfos = this.topicRegistry.get(topic);
                if (partRegInfos == null) {
                    partRegInfos = new ConcurrentHashMap<Partition, TopicPartitionRegInfo>();
                    this.topicRegistry.put(topic, new ConcurrentHashMap<Partition, TopicPartitionRegInfo>());
                }
                final Set<Partition> currentParts = partRegInfos.keySet();

                for (final Partition partition : currentParts) {
                    // 新的分区列表中不存在的分区，需要释放ownerShip，也就是老的有，新的没有
                    if (!newParts.contains(partition.toString())) {
                        log.warn("Releasing partition ownerships for partition:" + partition);
                        this.releasePartitionOwnership(topic, partition);
                        partRegInfos.remove(partition);
                    }
                }

                for (final String partition : newParts) {
                    // 当前没有的分区，挂载上去，也就是新的有，老的没有
                    if (!currentParts.contains(new Partition(partition))) {
                        log.warn(consumerId + " attempting to claim partition " + partition);
                        // 注册分区owner关系
                        if (!this.ownPartition(topicDirs, partition, topic, consumerId)) {
                            log.warn("Claim partition " + partition + " failed,retry...");
                            return false;
                        }
                    }
                }

            }
            this.updateFetchRunner(cluster);
            this.oldPartitionsPerTopicMap = partitionsPerTopicMap;
            this.oldConsumersPerTopicMap = consumersPerTopicMap;
            this.oldCluster = cluster;

            return true;
        }
        // 关闭连接到MQ的服务的客户端
        private void closeOldBrokersConnections() throws NotifyRemotingException {
            for (Broker old : this.oldBrokerSet) {
                ConsumerZooKeeper.this.remotingClient.closeWithRef(old.getZKString(), this, false);
                log.warn("Closed " + old.getZKString());
            }
        }

        protected boolean checkClusterChange(final Cluster cluster) {
            return !this.oldCluster.equals(cluster);
        }

        /**
         * 获取zk上注册的topic到partition映射的map. 包括master和slave的所有partitions
         *
         * @param myConsumerPerTopicMap
         * @return
         */
        protected Map<String, List<String>> getPartitionStringsForTopics(final Map<String, String> myConsumerPerTopicMap) {
            return ConsumerZooKeeper.this.metaZookeeper.getPartitionStringsForSubTopics(myConsumerPerTopicMap.keySet());
        }

        /**
         * 添加分区的owner关系
         * 
         * @param topicDirs
         * @param partition
         * @param topic
         * @param consumerThreadId
         * @return
         */
        protected boolean ownPartition(final ZKGroupTopicDirs topicDirs, final String partition, final String topic, final String consumerThreadId) throws Exception {
            final String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition;
            try {
                ZkUtils.createEphemeralPathExpectConflict(ConsumerZooKeeper.this.zkClient, partitionOwnerPath,
                    consumerThreadId);
            }
            catch (final ZkNodeExistsException e) {
                // 原始的关系应该已经删除，所以稍候再重试
                log.info("waiting for the partition ownership to be deleted: " + partition);
                return false;

            }
            catch (final Exception e) {
                throw e;
            }
            this.addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
            return true;
        }

        // 获取offset信息并保存到本地
        protected void addPartitionTopicInfo(final ZKGroupTopicDirs topicDirs, final String partitionString, final String topic, final String consumerThreadId) {
            final Partition partition = new Partition(partitionString);
            final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partitionTopicInfo =
                    this.topicRegistry.get(topic);
            TopicPartitionRegInfo existsTopicPartitionRegInfo = this.loadTopicPartitionRegInfo(topic, partition);
            if (existsTopicPartitionRegInfo == null) {
                // 初始化的时候默认使用0,TODO 可能采用其他
                existsTopicPartitionRegInfo =
                        this.initTopicPartitionRegInfo(topic, consumerThreadId, partition,
                            this.consumerConfig.getOffset());// Long.MAX_VALUE
            }
            // If alwaysConsumeFromMaxOffset is set to be true,we always set
            // offset to be Long.MAX_VALUE
            if (this.consumerConfig.isAlwaysConsumeFromMaxOffset()) {
                existsTopicPartitionRegInfo.getOffset().set(Long.MAX_VALUE);
            }
            partitionTopicInfo.put(partition, existsTopicPartitionRegInfo);
        }

        /**
         * 释放分区所有权
         */
        private void releaseAllPartitionOwnership() {
            for (final Map.Entry<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> entry : this.topicRegistry
                    .entrySet()) {
                final String topic = entry.getKey();
                final ZKGroupTopicDirs topicDirs =
                        ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
                for (final Partition partition : entry.getValue().keySet()) {
                    final String znode = topicDirs.consumerOwnerDir + "/" + partition;
                    this.deleteOwnership(znode);
                }
            }
        }

        /**
         * 释放指定分区的ownership
         * 
         * @param topic
         * @param partition
         */
        private void releasePartitionOwnership(final String topic, final Partition partition) {
            final ZKGroupTopicDirs topicDirs =
                    ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
            final String znode = topicDirs.consumerOwnerDir + "/" + partition;
            this.deleteOwnership(znode);
        }

        private void deleteOwnership(final String znode) {
            try {
                ZkUtils.deletePath(ConsumerZooKeeper.this.zkClient, znode);
            }
            catch (final Throwable t) {
                log.error("exception during releasePartitionOwnership", t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Consumer " + this.consumerIdString + " releasing " + znode);
            }
        }

        /**
         * 释放指定topic关联分区的ownership
         * 
         * @param topic
         */
        private void releasePartitionOwnership(final String topic) {
            final ZKGroupTopicDirs topicDirs =
                    ConsumerZooKeeper.this.metaZookeeper.new ZKGroupTopicDirs(topic, this.consumerConfig.getGroup());
            final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partInfos = this.topicRegistry.get(topic);
            if (partInfos != null) {
                for (final Partition partition : partInfos.keySet()) {
                    final String znode = topicDirs.consumerOwnerDir + "/" + partition;
                    this.deleteOwnership(znode);
                }
            }
        }

        /**
         * 返回有变更的topic跟consumer集合
         * 
         * @param myConsumerPerTopicMap Map<消费者, 消费者订阅的topic>
         * @param newPartMap            变更后：Map<topic, topic的可用分区>
         * @param oldPartMap            变更前：Map<topic, topic的可用分区>
         * @param newConsumerMap        变更后：Map<topic, topic的订阅者>
         * @param oldConsumerMap        变更前：Map<topic, topic的订阅者>
         * @return
         */
        private Map<String, String> getRelevantTopicMap(
                final Map<String, String> myConsumerPerTopicMap,
                final Map<String, List<String>> newPartMap,
                final Map<String, List<String>> oldPartMap,
                final Map<String, List<String>> newConsumerMap,
                final Map<String, List<String>> oldConsumerMap) {

            // Map<topic, topic的订阅者>
            final Map<String, String> relevantTopicThreadIdsMap = new HashMap<String, String>();

            for (final Map.Entry<String, String> entry : myConsumerPerTopicMap.entrySet()) {
                final String topic = entry.getKey();
                final String consumerId = entry.getValue();
                // 判断分区变更或者订阅者列表是否变更
                if (!this.listEquals(oldPartMap.get(topic), newPartMap.get(topic))
                        || !this.listEquals(oldConsumerMap.get(topic), newConsumerMap.get(topic))) {
                    relevantTopicThreadIdsMap.put(topic, consumerId);
                }
            }
            return relevantTopicThreadIdsMap;
        }
        // 比较两个集合里的元素是否一样
        private boolean listEquals(final List<String> list1, final List<String> list2) {
            if (list1 == null && list2 != null) {
                return false;
            }
            if (list1 != null && list2 == null) {
                return false;
            }
            if (list1 == null && list2 == null) {
                return true;
            }
            return list1.equals(list2);
        }

        /**
         * 获取某个分组订阅的topic到订阅者之间的映射map
         * 
         * @param group
         * @return
         * @throws Exception
         * @throws NoNodeException 多个consumer同时在负载均衡时,可能会抛出NoNodeException
         */
        protected Map<String, List<String>> getConsumersPerTopic(final String group) throws Exception, NoNodeException {
            final List<String> consumers =
                    ZkUtils.getChildren(ConsumerZooKeeper.this.zkClient, this.dirs.consumerRegistryDir);
            if (consumers == null) {
                return Collections.emptyMap();
            }
            final Map<String, List<String>> consumersPerTopicMap = new HashMap<String, List<String>>();
            for (final String consumer : consumers) {
                final List<String> topics = this.getTopics(consumer);// 多个consumer同时在负载均衡时,这里可能会抛出NoNodeException，--wuhua
                for (final String topic : topics) {
                    if (consumersPerTopicMap.get(topic) == null) {
                        final List<String> list = new ArrayList<String>();
                        list.add(consumer);
                        consumersPerTopicMap.put(topic, list);
                    }
                    else {
                        consumersPerTopicMap.get(topic).add(consumer);
                    }
                }

            }
            // 订阅者排序
            for (final Map.Entry<String, List<String>> entry : consumersPerTopicMap.entrySet()) {
                Collections.sort(entry.getValue());
            }
            return consumersPerTopicMap;
        }

        /**
         * 根据consumerId获取订阅的topic列表
         * 
         * @param consumerId
         * @return
         * @throws Exception
         */
        protected List<String> getTopics(final String consumerId) throws Exception {
            final String topicsString =
                    ZkUtils.readData(ConsumerZooKeeper.this.zkClient, this.dirs.consumerRegistryDir + "/" + consumerId);
            if (StringUtils.isBlank(topicsString)) {
                return Collections.emptyList();
            }
            final String[] topics = topicsString.split(",");
            final List<String> rt = new ArrayList<String>(topics.length);
            for (final String topic : topics) {
                rt.add(topic);
            }
            return rt;
        }
    }
}