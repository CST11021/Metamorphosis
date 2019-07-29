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
package com.taobao.metamorphosis.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Cluster;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.cluster.json.TopicBroker;


/**
 * Meta与zookeeper交互的辅助类：用于获取broker在zk上注册的信息，比如：主从MQ服务器的brokerId，topic等信息
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-15
 * 
 */
public class MetaZookeeper {

    private static Log logger = LogFactory.getLog(MetaZookeeper.class);

    static {
        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.warn("Thread terminated with exception: "+ t.getName(),e);
                }
            });
        }
    }

    // zk客户端
    private volatile ZkClient zkClient;

    //** 表示MQ在zk上的根节点，默认是/meta，来源于{@link ZkUtils.ZKConfig#zkRoot} */
    public final String metaRoot;
    // 消费者在zk上的路径：/meta/consumers
    public final String consumersPath;
    // brokerId在zk的路径
    // 例如：/meta/brokers/ids/0，这里0表示broken的id，爱该路径下可能还有从MQ服务器
    // path = /meta/brokers/ids/0/master	value = meta://192.168.13.158:8123
    // path = /meta/brokers/ids/0/salve     value = ...
    public final String brokerIdsPath;
    // topic在zk上的路径，例如：/meta/brokers/test_topic，
    @Deprecated
    public final String brokerTopicsPath;
    // 当topic发布完成后，会记录在该路径节点     added by dennis,sinace 1.4.3
    public final String brokerTopicsPubPath;
    // 当topic被订阅后，会记录在该路径下，/meta/brokers/topics-sub/
    // 1、brokerTopicsSubPath + ${topic} 就是topic在zk上的注册信息，当consumer完成订阅时，会监听brokerTopicsSubPath的子节点变更情况，一旦发现topic的订阅信息发生更变，则会重复均衡consumer的消费负载
    // 2、brokerTopicsSubPath + ${topic} + "/" + ${brokerId} 路径则记录的是topic在各个broker上的分区信息，保存的信息对应 TopicBroker 类
    public final String brokerTopicsSubPath;

    public MetaZookeeper(final ZkClient zkClient, final String root) {
        this.zkClient = zkClient;
        // metaRoot默认为：/meta
        this.metaRoot = this.normalize(root);
        this.consumersPath = this.metaRoot + "/consumers";
        this.brokerIdsPath = this.metaRoot + "/brokers/ids";
        this.brokerTopicsPath = this.metaRoot + "/brokers/topics";
        this.brokerTopicsPubPath = this.metaRoot + "/brokers/topics-pub";
        this.brokerTopicsSubPath = this.metaRoot + "/brokers/topics-sub";
    }

    /**
     * 犯规化root节点的路径，以"/"开始，并且不以"/"结束，例如：
     * mate/test/       =>      /mate/test
     * /mate            =>      /mate
     * mate/            =>      /mate
     *
     * @param root
     * @return
     */
    private String normalize(final String root) {
        if (root.startsWith("/")) {
            return this.removeLastSlash(root);
        }
        else {
            return "/" + this.removeLastSlash(root);
        }
    }

    /**
     * 如果是以"/"结尾的，则移除"/"
     *
     * @param root
     * @return
     */
    private String removeLastSlash(final String root) {
        if (root.endsWith("/")) {
            return root.substring(0, root.lastIndexOf("/"));
        }
        else {
            return root;
        }
    }

    /**
     * 返回broker集群,包含slave和master
     * 
     * @return
     */
    public Cluster getCluster() {
        final Cluster cluster = new Cluster();
        // 获取"/meta/brokers/ids/"路径下的子节点
        final List<String> nodes = ZkUtils.getChildren(this.zkClient, this.brokerIdsPath);
        for (final String node : nodes) {
            // String brokerZKString = readData(zkClient, brokerIdsPath + "/" + node);
            final int brokerId = Integer.parseInt(node);
            final Set<Broker> brokers = this.getBrokersById(brokerId);
            if (brokers != null && !brokers.isEmpty()) {
                cluster.addBroker(brokerId, brokers);
            }
        }
        return cluster;
    }

    /**
     * 从zk查询一个id下的brokers,包含master和一个或多个slave
     * @param brokerId
     * @return
     */
    public Set<Broker> getBrokersById(final int brokerId) {
        final Set<Broker> set = new HashSet<Broker>();
        final Broker masterBroker = this.getMasterBrokerById(brokerId);
        final Set<Broker> slaveBrokers = this.getSlaveBrokersById(brokerId);
        if (masterBroker != null) {
            set.add(masterBroker);
        }
        if (slaveBrokers != null && !slaveBrokers.isEmpty()) {
            set.addAll(slaveBrokers);
        }
        return set;
    }

    /**
     * 从zk查询master broker,不存在则返回null
     * @param brokerId
     * @return
     */
    public Broker getMasterBrokerById(final int brokerId) {
        final String brokersString = ZkUtils.readDataMaybeNull(this.zkClient, this.brokerIdsPathOf(brokerId, -1));
        if (StringUtils.isNotBlank(brokersString)) {
            return new Broker(brokerId, brokersString);
        }
        return null;
    }

    /**
     * 从zk查询slave broker,不存在则返回null,
     * @param brokerId
     * @return
     */
    private Set<Broker> getSlaveBrokersById(final int brokerId) {
        final Set<Broker> ret = new HashSet<Broker>();
        final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerIdsPath + "/" + brokerId);
        if (brokers == null) {
            return ret;
        }
        for (final String broker : brokers) {
            if (broker.startsWith("slave")) {
                int slaveId = -1;
                try {
                    // 去掉broker前面的5个字符
                    slaveId = Integer.parseInt(broker.substring(5));
                    if (slaveId < 0) {
                        logger.warn("skip invalid slave path:" + broker);
                        continue;
                    }
                }
                catch (final Exception e) {
                    logger.warn("skip invalid slave path:" + broker);
                    continue;
                }
                final String brokerData = ZkUtils.readDataMaybeNull(this.zkClient, this.brokerIdsPath + "/" + brokerId + "/" + broker);
                if (StringUtils.isNotBlank(brokerData)) {
                    ret.add(new Broker(brokerId, brokerData + "?slaveId=" + slaveId));
                }
            }
        }
        return ret;
    }

    /**
     * 返回发布了指定的topic的所有master brokers
     * @param topic
     * @return Map<brokerId, broker节点数据字符串如meta://host:port>
     */
    public Map<Integer, String> getMasterBrokersByTopic(final String topic) {
        final Map<Integer, String> ret = new TreeMap<Integer, String>();
        final List<String> brokerIds = ZkUtils.getChildren(this.zkClient, this.brokerTopicsPubPath + "/" + topic);
        if (brokerIds == null) {
            return ret;
        }
        for (final String brokerIdStr : brokerIds) {
            if (!brokerIdStr.endsWith("-m")) {
                continue;
            }
            final int brokerId = Integer.parseInt(StringUtils.split(brokerIdStr, "-")[0]);
            final Broker broker = this.getMasterBrokerById(brokerId);
            if (broker != null) {
                ret.put(brokerId, broker.getZKString());
            }
        }
        return ret;

    }

    /**
     * 返回master的topic到partition映射的map
     * 
     * @param topics
     * @param topics
     * @return
     */
    public Map<String, List<Partition>> getPartitionsForTopicsFromMaster(final Collection<String> topics) {
        final Map<String, List<Partition>> ret = new HashMap<String, List<Partition>>();
        for (final String topic : topics) {
            List<Partition> partList = null;
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsPubPath + "/" + topic);
            for (final String broker : brokers) {
                final String[] brokerStrs = StringUtils.split(broker, "-");
                if (this.isMaster(brokerStrs)) {
                    String path = this.brokerTopicsPubPath + "/" + topic + "/" + broker;
                    String brokerData = ZkUtils.readData(this.zkClient, path);
                    try {
                        final TopicBroker topicBroker = TopicBroker.parse(brokerData);
                        if (topicBroker == null) {
                            logger.warn("Null broker data for path:" + path);
                            continue;
                        }
                        for (int part = 0; part < topicBroker.getNumParts(); part++) {
                            if (partList == null) {
                                partList = new ArrayList<Partition>();
                            }
                            final Partition partition = new Partition(Integer.parseInt(brokerStrs[0]), part);
                            if (!partList.contains(partition)) {
                                partList.add(partition);
                            }
                        }
                    }
                    catch (Exception e) {
                        logger.error("A serious error occurred,could not parse broker data at path=" + path
                            + ",and broker data is:" + brokerData, e);
                    }
                }
            }
            if (partList != null) {
                Collections.sort(partList);
                ret.put(topic, partList);
            }
        }
        return ret;
    }
    private boolean isMaster(final String[] brokerStrs) {
        return brokerStrs != null && brokerStrs.length == 2 && brokerStrs[1].equals("m");
    }

    /**
     * 返回指定id的master broker上所有被订阅的topic
     * @param brokerId
     * @return
     */
    public Set<String> getTopicsByBrokerIdFromMaster(final int brokerId) {
        final Set<String> set = new HashSet<String>();
        final List<String> allTopics = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath);
        for (final String topic : allTopics) {
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath + "/" + topic);
            if (brokers != null && brokers.size() > 0) {
                for (final String broker : brokers) {
                    if ((String.valueOf(brokerId) + "-m").equals(broker)) {
                        set.add(topic);
                    }
                }
            }
        }
        return set;
    }

    /**
     * 返回一个master 下的topic到partition映射的map
     * 
     * @param topics
     * @param brokerId
     * @return
     */
    public Map<String, List<Partition>> getPartitionsForSubTopicsFromMaster(final Collection<String> topics, final int brokerId) {
        final Map<String, List<Partition>> ret = new HashMap<String, List<Partition>>();
        if (topics != null) {
            for (final String topic : topics) {
                List<Partition> partList = null;
                // 获取指定brokerId的master的订阅topic路径，客户端订阅topic时，会记录在zk上
                final String dataString =
                        ZkUtils.readDataMaybeNull(this.zkClient, this.brokerTopicsPathOf(topic, false, brokerId, -1));
                if (StringUtils.isBlank(dataString)) {
                    continue;
                }

                try {
                    final TopicBroker topicBroker = TopicBroker.parse(dataString);
                    if (topicBroker == null) {
                        continue;
                    }

                    for (int part = 0; part < topicBroker.getNumParts(); part++) {
                        if (partList == null) {
                            partList = new ArrayList<Partition>();
                        }
                        partList.add(new Partition(brokerId, part));
                    }
                    if (partList != null) {
                        Collections.sort(partList);
                        ret.put(topic, partList);
                    }
                }
                catch (Exception e) {
                    throw new IllegalStateException("Parse data to TopicBroker failed,data is:" + dataString, e);
                }
            }
        }
        return ret;
    }

    /**
     * 返回一个master下的topic到partition映射的map
     * 
     * @param topics
     * @param brokerId
     * @return Map<topic, ${brokenId}-${分区索引}>
     */
    public Map<String, List<String>> getPartitionStringsForSubTopicsFromMaster(final Collection<String> topics, final int brokerId) {
        final Map<String, List<String>> ret = new HashMap<String, List<String>>();
        final Map<String, List<Partition>> tmp = this.getPartitionsForSubTopicsFromMaster(topics, brokerId);
        if (tmp != null && !tmp.isEmpty()) {
            for (final Map.Entry<String, List<Partition>> each : tmp.entrySet()) {
                final String topic = each.getKey();
                List<String> list = ret.get(topic);
                if (list == null) {
                    list = new ArrayList<String>();
                }
                for (final Partition partition : each.getValue()) {
                    list.add(partition.getBrokerId() + "-" + partition.getPartition());
                }
                if (list != null) {
                    Collections.sort(list);
                    ret.put(topic, list);
                }
            }
        }
        return ret;
    }

    /**
     * 返回topic到partition映射的map. 包括master和slave的所有partitions
     * 
     * @param topics
     * @return 返回Map<topic, List<brokerId-分区索引>> 例如：{"meta-test": ["0-0", "0-1", "0-2"]} 表示"meta-test"这个topic在brokerId为0的MQ服务器上对应了三个分区索引
     */
    public Map<String, List<String>> getPartitionStringsForSubTopics(final Collection<String> topics) {
        final Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (final String topic : topics) {
            List<String> partList = null;
            final List<String> brokers = ZkUtils.getChildren(this.zkClient, this.brokerTopicsSubPath + "/" + topic);
            for (final String broker : brokers) {
                final String[] tmp = StringUtils.split(broker, "-");
                if (tmp != null && tmp.length == 2) {
                    String path = this.brokerTopicsSubPath + "/" + topic + "/" + broker; // 例如：/meta/brokers/topics-sub/meta-test/0-m
                    String brokerData = ZkUtils.readData(this.zkClient, path);
                    try {
                        final TopicBroker topicBroker = TopicBroker.parse(brokerData);
                        if (topicBroker == null) {
                            logger.warn("Null broker data for path:" + path);
                            continue;
                        }
                        for (int part = 0; part < topicBroker.getNumParts(); part++) {
                            if (partList == null) {
                                partList = new ArrayList<String>();
                            }

                            final String partitionString = tmp[0] + "-" + part;
                            if (!partList.contains(partitionString)) {
                                partList.add(partitionString);
                            }
                        }
                    }
                    catch (Exception e) {
                        logger.error("A serious error occurred,could not parse broker data at path=" + path
                            + ",and broker data is:" + brokerData, e);
                    }
                }
                else {
                    logger.warn("skip invalid topics path:" + broker);
                }
            }
            if (partList != null) {
                Collections.sort(partList);
                ret.put(topic, partList);
            }
        }
        return ret;
    }

    /**
     * brokerId 在zk上注册的path，例如：
     * /meta/brokers/ids/0/master
     * /meta/brokers/ids/0/slave
     * 
     * @param brokerId
     * @param slaveId slave编号, 小于0表示master
     * 
     */
    public String brokerIdsPathOf(final int brokerId, final int slaveId) {
        return this.brokerIdsPath + "/" + brokerId + (slaveId >= 0 ? "/slave" + slaveId : "/master");
    }

    /**
     * Master config file checksum path
     * 返回broker的在zk上 master_config_checksum 节点的路径
     * 
     * @param brokerId
     * @return
     */
    public String masterConfigChecksum(final int brokerId) {
        return this.brokerIdsPath + "/" + brokerId + "/master_config_checksum";
    }

    /**
     * topic 在zk上注册的path
     * 
     * @param topic
     * @param brokerId
     * @param slaveId slave编号, 小于0表示master
     * */
    @Deprecated
    public String brokerTopicsPathOf(final String topic, final int brokerId, final int slaveId) {
        return this.brokerTopicsPath + "/" + topic + "/" + brokerId + (slaveId >= 0 ? "-s" + slaveId : "-m");
    }

    /**
     * 返回topic在zk上的路径
     * 
     * @since 1.4.3
     * @param topic
     * @param publish       true：返回发布的topic路径；false：返回订阅的topic路径
     * @param brokerId
     * @param slaveId       slave编号, 小于0表示master
     * */
    public String brokerTopicsPathOf(final String topic, boolean publish, final int brokerId, final int slaveId) {
        String parent = publish ? this.brokerTopicsPubPath : this.brokerTopicsSubPath;
        return parent + "/" + topic + "/" + brokerId + (slaveId >= 0 ? "-s" + slaveId : "-m");
    }




    public ZkClient getZkClient() {
        return this.zkClient;
    }
    public void setZkClient(final ZkClient zkClient) {
        this.zkClient = zkClient;
    }



    /**
     * 封装了消息消费者在zk上group目录和ids节点信息
     */
    public class ZKGroupDirs {

        // 消费者路径，默认：/meta/consumers
        public String consumerDir = MetaZookeeper.this.consumersPath;
        // 消费者分组路径，默认：/meta/consumers/${分组名}
        public String consumerGroupDir;
        // 消费者的id路径，例如：/meta/consumers/${分组名}/ids，注意：/meta/consumers/${分组名}/ids/${consumerId} 节点保存了消费者订阅的topic信息，多个topic用","分隔
        public String consumerRegistryDir;

        public ZKGroupDirs(final String group) {
            this.consumerGroupDir = this.consumerDir + "/" + group;
            this.consumerRegistryDir = this.consumerGroupDir + "/ids";
        }


    }

    /**
     * 封装了消息消费者在zk上相关目录
     */
    public class ZKGroupTopicDirs extends ZKGroupDirs {
        public ZKGroupTopicDirs(final String topic, final String group) {
            super(group);
            // /meta/consumers/${分组名}/offsets/${topic}
            this.consumerOffsetDir = this.consumerGroupDir + "/offsets/" + topic;
            // topic路径：/meta/consumers/${分组名}/owners/${topic}
            // 分区的路径：/meta/consumers/${分组名}/owners/${topic}/${partition} 该节点为数据节点，节点保存了consumerId，表明该分区只能被对应consumerId消费，注意：一个分区只能被一个consumerId消费，但是一个consumerId可以消费多个分区，对应的策略参考 LoadBalanceStrategy 类
            this.consumerOwnerDir = this.consumerGroupDir + "/owners/" + topic;
        }

        public String consumerOffsetDir;
        public String consumerOwnerDir;
    }
}