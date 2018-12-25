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
package com.taobao.metamorphosis.server.utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.utils.Config;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * MateQ服务器端配置
 *
 * @author boyan
 * @author wuhua
 * @Date 2011-4-21
 */
public class MetaConfig extends Config implements Serializable, MetaConfigMBean {

    static final Log log = LogFactory.getLog(MetaConfig.class);

    static final long serialVersionUID = -1L;

    /** 服务器集群中唯一的id，必须为整型0-1024之间。对服务器集群的定义是使用同一个zookeeper并且在zookeeper上的root path相同，具体参见zookeeper配置 */
    private int brokerId = 0;
    /** 用户目录下的meta目录, 表示消息存储在MQ服务器上的磁盘目录 */
    private String dataPath = System.getProperty("user.home") + File.separator + "meta";
    /** MQ默认的端口 */
    private int serverPort = 8123;
    /** 控制板端口 */
    private int dashboardHttpPort = 8120;
    /** 表示MQ服务器所在的机器, 如果没有配置，默认为本地IP地址 */
    private String hostName;
    /**
     * topic分区数，每个topic在MQ服务器上可以有多个分区，当生产者想MQ服务器发送消息时，如果客户端没有指定的分区（客户端没有指定分区时，
     * 请求中的分区索引默认是-1），则服务器会从可用（分区是可以被关闭的）的分区中，随机选择一个分区来保存消息
     */
    private int numPartitions = 1;
    /**
     * 每隔多少条消息做一次磁盘同步，强制将更改的数据刷入磁盘。默认为1000条。也就是说在掉电情况下，最多允许丢失1000条消息。
     * 可设置为1，强制每次写入立即同步到磁盘。在<=0的情况下，服务器会自动启用group commit技术，将多个消息合并成一次再同步来提升IO性能。
     * 经过测试，group commit情况下消息发送者的TPS没有受到太大影响，但是服务端的负载会上升很多。
     *
     * 判断是否启用异步写入：
     * 1、如果设置为unflushThreshold <=0的数字，则认为启动异步写入；
     * 2、如果设置为unflushThreshold = 1，则是同步写入，即每写入一个消息都会提交到磁盘；
     * 3、如果unflushThreshold > 0，则是依赖组提交或者是超时提交
     *
     * 客户端每次put消息到MQ服务器时，服务器都判断是否要将该消息立即写入磁盘，判断规则如下：
     * 如果这个topic的消息的最后写入磁盘的时间 > 配置时间（unflushInterval） 或者 这个topic还没写入磁盘的消息数量 > 配置的数量（unflushThreshold），则立即写入磁盘
     */
    private int unflushThreshold = 1000;
    /**
     * 间隔多少毫秒定期做一次磁盘sync（将消息保存到磁盘）,默认是10秒。也就是说在服务器掉电情况下，最多丢失10秒内发送过来的消息。不可设置为小于或者等于0。
     * MessageStoreManager#FlushRunner会定期将消息管理器中的消息flush到磁盘
     */
    private int unflushInterval = 10000;
    /** 表示单个消息数据文件的最大大小，默认为1G。默认无需修改此选项 */
    private int maxSegmentSize = 1 * 1024 * 1024 * 1024;
    /** 传输给消费者的最大数据大小，默认为1M，请根据你的最大消息大小酌情设置，如果太小，每次无法传输一个完整的消息给消费者，导致消费者消费停滞。可设置成一个大数来取消限制 */
    private int maxTransferSize = 1024 * 1024;
    /** getProcessThreadCount: 处理get请求（就是消费者从MQ服务器拉取消息的请求）的并发线程数，默认为CPUS*10 */
    private int getProcessThreadCount = 10 * Runtime.getRuntime().availableProcessors();
    /** putProcessThreadCount: 处理put请求（就是生产者向MQ服务器发送消息的请求）的并发线程数，默认为CPUS*10 */
    private int putProcessThreadCount = 10 * Runtime.getRuntime().availableProcessors();
    /**
     * 文件删除策略:"策略名称,设定值列表"，默认为保存7天
     * deletePolicy: 数据删除策略，默认超过7天即删除,这里的168是小时，10s表示10秒，10m表示10分钟，10h表示10小时，不明确指定单位默认为小时。
     * delete是指删除，超过指定时间的数据文件将被彻底从磁盘删除。
     *
     * 也可以选择archive策略，即不对过期的数据文件做删除而是归档，当使用archive策略的时候可以选择是否压缩数据文件，如167,archive,true
     * 即选择将更改时间超过7天的数据文件归档并压缩为zip文件，如果不选择压缩，则重命名为扩展名为arc的文件。
     */
    private String deletePolicy = "delete,168";
    /**
     * PropertyChangeSupport对象用来监听对象的属性值变化：
     *      当bean的属性发生变化时，使用PropertyChangeSupport对象的firePropertyChange方法，会将一个事件发送给所有已经注册的监听器。
     *      该方法有三个参数：属性的名字、旧的值以及新的值。属性的值必须是对象，如果是简单数据类型，则必须进行包装）
     */
    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    /** 表示配置文件的最后修改时间 */
    private long lastModified = -1;
    /** 表示配置文件的绝对路径 */
    private volatile String path;
    /** App class path. */
    private String appClassPath;
    /**
     * 事务相关配置：最大保存的checkpoint数目，超过将淘汰最老的
     * checkPoint相关知识：
     * 在数据库系统中，写日志和写数据文件是数据库中IO消耗最大的两种操作，在这两种操作中写数据文件属于分散写，写日志文件是顺序写，因此为了保
     * 证数据库的性能，通常数据库都是保证在提交(commit)完成之前要先保证日志都被写入到日志文件中，而脏数据块则保存在数据缓存(buffer cache)
     * 中再不定期的分批写入到数据文件中。也就是说日志写入和提交操作是同步的，而数据写入和提交操作是不同步的。这样就存在一个问题，当一个数据
     * 库崩溃的时候并不能保证缓存里面的脏数据全部写入到数据文件中，这样在实例启动的时候就要使用日志文件进行恢复操作，将数据库恢复到崩溃之前
     * 的状态，保证数据的一致性。检查点是这个过程中的重要机制，通过它来确定，恢复时哪些重做日志应该被扫描并应用于恢复。
     */
    private int maxCheckpoints = 3;
    /** 自动checkpoint间隔，默认1小时 */
    private long checkpointInterval = 60 * 60 * 1000L;
    /** 最大事务超时时间个数，默认3万个 */
    private int maxTxTimeoutTimerCapacity = 30000;
    /** 事务日志刷盘设置，0表示让操作系统决定，1表示每次commit都刷盘，2表示每隔一秒刷盘一次 */
    private int flushTxLogAtCommit = 1;
    /** 事务最大超时时间，默认一分钟 */
    private int maxTxTimeoutInSeconds = 60;
    /** 日志存储目录，默认使用dataPath */
    private String dataLogPath = this.dataPath;
    /** 何时执行删除策略的cron表达式，默认是0 0 6,18 * * ?，也就是每天的早晚6点执行处理策略。*/
    private String deleteWhen = "0 0 6,18 * * ?";
    /** quartz使用的线程池大小 */
    private int quartzThreadCount = 5;
    /** 表示配置文件的一个校验值，只要文件内容有变更就会返回一个新的数值 */
    private long configFileChecksum;
    /** 是否接收消息，默认为true；如果为false，则不会注册发送信息到zookeeper上，客户端当然无法发送消息到该broker。本参数可以被后续的topic配置覆盖。**/
    private boolean acceptPublish = true;
    /** 与acceptPublish类似，默认也为true；如果为false，则不会注册消费信息到zookeeper上，消费者无法发现该broker，当然无法从该broker消费消息。本参数可以被后续的topic配置覆盖。*/
    private boolean acceptSubscribe = true;
    /** 全局性地控制是否开启实时统计，可被topic配置覆盖，默认为false */
    private boolean stat;
    /** 当消费者的offset不在Broker的数据范围内，是否强制更新消费者的offset为当前最大offset。默认为false。测试开发环境建议开启此选项，生产环境不建议。 */
    private boolean updateConsumerOffsets = Boolean.parseBoolean(System.getProperty("meta.get.tellMaxOffset", "false"));
    /** 是否并行加载消息存储，MQ服务器上的消息是被存储在磁盘，该属性标识是否启动时并行加载数据，开启可提升启动速度。默认不开启。开启后启动日志顺序可能紊乱 */
    private boolean loadMessageStoresInParallel = false;
    /** 用于保存配置文件中配置的topic */
    private List<String> topics = new ArrayList<String>();
    /** 保存配置文件中配置的topic及对应的TopicConfig，Map<topic, TopicConfig>*/
    private ConcurrentHashMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<String, TopicConfig>();
    /** Async slave config */
    private SlaveConfig slaveConfig;
    /** zk配置 */
    private ZKConfig zkConfig;
    /** Map<topic, partition> 表示topic和关闭的分区 */
    private final Map<String, Set<Integer>> closedPartitionMap = new CopyOnWriteMap<String, Set<Integer>>();


    /** 构造器 */
    public MetaConfig() {
        super();
        // 将配置注册到java的MBeanServer
        MetaMBeanServer.registMBean(this, null);
    }


    /**
     * 加载配置文件
     * @param path 配置文件路径
     */
    public void loadFromFile(final String path) {
        try {
            this.path = path;
            final File file = new File(path);

            // 表示配置文件父目录下provided目录
            File appClassDir = new File(file.getParentFile().getParentFile(), "provided");
            if (appClassDir.exists() && appClassDir.isDirectory()) {
                // It's a directory,it must be ends with "/"
                this.appClassPath = appClassDir.getAbsolutePath() + "/";
            }

            if (!file.exists()) {
                throw new MetamorphosisServerStartupException("File " + path + " is not exists");
            }

            // 将.ini的配置文件转为Ini对象
            final Ini conf = this.createIni(file);
            // 解析文件配置
            this.populateAttributes(conf);
        } catch (final IOException e) {
            throw new MetamorphosisServerStartupException("Parse configuration failed,path=" + path, e);
        }
    }

    /**
     * 从字符串中加载配置
     * @param str
     */
    public void loadFromString(final String str) {
        try {
            StringReader reader = new StringReader(str);
            final Ini conf = new Ini(reader);
            this.populateAttributes(conf);
        } catch (final IOException e) {
            throw new MetamorphosisServerStartupException("Parse configuration failed,path=" + this.path, e);
        }
    }

    /**
     * 创建.ini的配置文件转为的{@link Ini}对象
     * @param file
     * @return
     * @throws IOException
     * @throws InvalidFileFormatException
     */
    private Ini createIni(final File file) throws IOException, InvalidFileFormatException {
        final Ini conf = new Ini(file);
        this.lastModified = file.lastModified();
        // CRC32:CRC本身是“冗余校验码”的意思，CRC32则表示会产生一个32bit（8位十六进制数）的校验值。由于CRC32产生校验值时源数据块的每一
        // 个bit（位）都参与了计算，所以数据块中即使只有一位发生了变化，也会得到不同的CRC32值.
        this.configFileChecksum = org.apache.commons.io.FileUtils.checksumCRC32(file);
        this.propertyChangeSupport.firePropertyChange("configFileChecksum", null, null);
        return conf;
    }

    /**
     * Ini表示一个.ini的配置文件，该方法用于解析配置文件
     * @param conf
     */
    protected void populateAttributes(final Ini conf) {
        // 解析[system]下的配置
        this.populateSystemConf(conf);
        // 解析[zookeeper]下的配置
        this.populateZookeeperConfig(conf);
        // 解析topic相关的配置
        this.populateTopicsConfig(conf);
    }

    /**
     * 解析系统（[system]标签下的）相关的配置，例如配置中的如下配置：
     * [system]
     * brokerId=0
     * numPartitions=1
     * serverPort=8123
     * unflushThreshold=0
     * unflushInterval=10000
     * maxSegmentSize=1073741824
     * maxTransferSize=1048576
     * deletePolicy=delete,168
     * deleteWhen=0 0 6,18 * * ?
     * flushTxLogAtCommit=1
     * stat=true
     * @param conf
     */
    private void populateSystemConf(final Ini conf) {
        final Section sysConf = conf.get("system");

        Set<String> configKeySet = sysConf.keySet();
        Set<String> validKeySet = this.getFieldSet();
        this.checkConfigKeys(configKeySet, validKeySet);

        this.brokerId = this.getInt(sysConf, "brokerId");
        this.serverPort = this.getInt(sysConf, "serverPort", 8123);
        this.dashboardHttpPort = this.getInt(sysConf, "dashboardHttpPort", 8120);
        if (!StringUtils.isBlank(sysConf.get("dataPath"))) {
            this.setDataPath(sysConf.get("dataPath"));
        }
        if (!StringUtils.isBlank(sysConf.get("appClassPath"))) {
            this.appClassPath = sysConf.get("appClassPath");
        }
        if (!StringUtils.isBlank(sysConf.get("dataLogPath"))) {
            this.dataLogPath = sysConf.get("dataLogPath");
        }
        if (!StringUtils.isBlank(sysConf.get("hostName"))) {
            this.hostName = sysConf.get("hostName");
        }
        this.numPartitions = this.getInt(sysConf, "numPartitions");
        this.unflushThreshold = this.getInt(sysConf, "unflushThreshold");
        this.unflushInterval = this.getInt(sysConf, "unflushInterval");
        this.maxSegmentSize = this.getInt(sysConf, "maxSegmentSize");
        this.maxTransferSize = this.getInt(sysConf, "maxTransferSize");
        if (!StringUtils.isBlank(sysConf.get("getProcessThreadCount"))) {
            this.getProcessThreadCount = this.getInt(sysConf, "getProcessThreadCount");
        }
        if (!StringUtils.isBlank(sysConf.get("putProcessThreadCount"))) {
            this.putProcessThreadCount = this.getInt(sysConf, "putProcessThreadCount");
        }
        if (!StringUtils.isBlank(sysConf.get("deletePolicy"))) {
            this.deletePolicy = sysConf.get("deletePolicy");
        }
        if (!StringUtils.isBlank(sysConf.get("deleteWhen"))) {
            this.deleteWhen = sysConf.get("deleteWhen");
        }
        if (!StringUtils.isBlank(sysConf.get("quartzThreadCount"))) {
            this.quartzThreadCount = this.getInt(sysConf, "quartzThreadCount");
        }
        if (!StringUtils.isBlank(sysConf.get("maxCheckpoints"))) {
            this.maxCheckpoints = this.getInt(sysConf, "maxCheckpoints");
        }
        if (!StringUtils.isBlank(sysConf.get("checkpointInterval"))) {
            this.checkpointInterval = this.getLong(sysConf, "checkpointInterval");
        }
        if (!StringUtils.isBlank(sysConf.get("maxTxTimeoutTimerCapacity"))) {
            this.maxTxTimeoutTimerCapacity = this.getInt(sysConf, "maxTxTimeoutTimerCapacity");
        }
        if (!StringUtils.isBlank(sysConf.get("flushTxLogAtCommit"))) {
            this.flushTxLogAtCommit = this.getInt(sysConf, "flushTxLogAtCommit");
        }
        if (!StringUtils.isBlank(sysConf.get("maxTxTimeoutInSeconds"))) {
            this.maxTxTimeoutInSeconds = this.getInt(sysConf, "maxTxTimeoutInSeconds");
        }

        // added by dennis,2012-05-19
        if (!StringUtils.isBlank(sysConf.get("acceptSubscribe"))) {
            this.acceptSubscribe = this.getBoolean(sysConf, "acceptSubscribe");
        }
        if (!StringUtils.isBlank(sysConf.get("acceptPublish"))) {
            this.acceptPublish = this.getBoolean(sysConf, "acceptPublish");
        }
        // added by dennis,2012-06-21
        if (!StringUtils.isBlank(sysConf.get("stat"))) {
            this.stat = this.getBoolean(sysConf, "stat");
        }
        if (!StringUtils.isBlank(sysConf.get("updateConsumerOffsets"))) {
            this.updateConsumerOffsets = this.getBoolean(sysConf, "updateConsumerOffsets");
        }
        if (!StringUtils.isBlank(sysConf.get("loadMessageStoresInParallel"))) {
            this.loadMessageStoresInParallel = this.getBoolean(sysConf, "loadMessageStoresInParallel");
        }
    }

    /**
     * 解析zookeeper相关的配置，例如：
     *
     * [zookeeper]
     * zk.zkConnect=localhost:2181
     * zk.zkSessionTimeoutMs=30000
     * zk.zkConnectionTimeoutMs=30000
     * zk.zkSyncTimeMs=5000
     * @param conf
     */
    private void populateZookeeperConfig(final Ini conf) {
        final Section zkConf = conf.get("zookeeper");
        Set<String> configKeySet = zkConf.keySet();
        Set<String> validKeySet = new ZKConfig().getFieldSet();
        validKeySet.addAll(this.getFieldSet());
        this.checkConfigKeys(configKeySet, validKeySet);
        if (!StringUtils.isBlank(zkConf.get("zk.zkConnect"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkConnect = zkConf.get("zk.zkConnect");
        }
        if (!StringUtils.isBlank(zkConf.get("zk.zkSessionTimeoutMs"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkSessionTimeoutMs = this.getInt(zkConf, "zk.zkSessionTimeoutMs");
        }
        if (!StringUtils.isBlank(zkConf.get("zk.zkConnectionTimeoutMs"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkConnectionTimeoutMs = this.getInt(zkConf, "zk.zkConnectionTimeoutMs");
        }
        if (!StringUtils.isBlank(zkConf.get("zk.zkSyncTimeMs"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkSyncTimeMs = this.getInt(zkConf, "zk.zkSyncTimeMs");
        }
        if (!StringUtils.isBlank(zkConf.get("zk.zkEnable"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkEnable = this.getBoolean(zkConf, "zk.zkEnable");
        }
        if (!StringUtils.isBlank(zkConf.get("zk.zkRoot"))) {
            this.newZkConfigIfNull();
            this.zkConfig.zkRoot = zkConf.get("zk.zkRoot");
        }
    }

    /**
     * 将配合文件的配置参数同步到内存，并通过{@link PropertyChangeSupport}机制发送变更通知
     * @param conf
     */
    private void populateTopicsConfig(final Ini conf) {
        final Set<String> set = conf.keySet();

        // 用于保存配置文件中配置的topic
        final List<String> newTopics = new ArrayList<String>();
        // 用于保存配置文件中topic对应的配置信息，Map<topic, TopicConfig>
        final ConcurrentHashMap<String, TopicConfig> newTopicConfigMap = new ConcurrentHashMap<String, TopicConfig>();

        for (final String name : set) {
            // 遍历topic相关配置
            if (name != null && name.startsWith("topic=")) {
                final Section section = conf.get(name);
                final String topic = name.substring("topic=".length());

                final TopicConfig topicConfig = new TopicConfig(topic, this);

                // TopicConfig中定义的配置项
                Set<String> validKeySet = topicConfig.getFieldSet();
                Set<String> allKeySet = section.keySet();
                Set<String> filterClassKeys = new HashSet<String>();

                // 配置文件中定义的配置项
                Set<String> configKeySet = new HashSet<String>();

                // 过滤配置文件中的配置项
                for (String key : allKeySet) {
                    if (key.startsWith("group.")) {
                        filterClassKeys.add(key);
                    } else {
                        configKeySet.add(key);
                    }
                }
                // 校验配置文件中的配置项有没有写错
                this.checkConfigKeys(configKeySet, validKeySet);

                if (StringUtils.isNotBlank(section.get("numPartitions"))) {
                    topicConfig.setNumPartitions(this.getInt(section, "numPartitions"));
                }
                if (StringUtils.isNotBlank(section.get("stat"))) {
                    topicConfig.setStat(Boolean.valueOf(section.get("stat")));
                }
                if (StringUtils.isNotBlank(section.get("deletePolicy"))) {
                    topicConfig.setDeletePolicy(section.get("deletePolicy"));
                }
                if (StringUtils.isNotBlank(section.get("deleteWhen"))) {
                    topicConfig.setDeleteWhen(section.get("deleteWhen"));
                }
                if (StringUtils.isNotBlank(section.get("dataPath"))) {
                    topicConfig.setDataPath(section.get("dataPath"));
                }
                if (StringUtils.isNotBlank(section.get("unflushInterval"))) {
                    topicConfig.setUnflushInterval(this.getInt(section, "unflushInterval"));
                }
                if (StringUtils.isNotBlank(section.get("unflushThreshold"))) {
                    topicConfig.setUnflushThreshold(this.getInt(section, "unflushThreshold"));
                }
                // added by dennis,2012-05-19
                if (!StringUtils.isBlank(section.get("acceptSubscribe"))) {
                    topicConfig.setAcceptSubscribe(this.getBoolean(section, "acceptSubscribe"));
                }
                if (!StringUtils.isBlank(section.get("acceptPublish"))) {
                    topicConfig.setAcceptPublish(this.getBoolean(section, "acceptPublish"));
                }

                // Added filter class
                for (String key : filterClassKeys) {
                    String consumerGroup = key.substring(6);
                    if (!StringUtils.isBlank(section.get(key))) {
                        topicConfig.addFilterClass(consumerGroup, section.get(key));
                    }
                }

                // this.topicPartitions.put(topic, numPartitions);
                newTopicConfigMap.put(topic, topicConfig);
                newTopics.add(topic);
            }
        }
        Collections.sort(newTopics);
        if (!newTopicConfigMap.equals(this.topicConfigMap)) {
            this.topics = newTopics;
            this.topicConfigMap = newTopicConfigMap;
            // 发送配置参数变更通知
            this.propertyChangeSupport.firePropertyChange("topics", null, null);
            this.propertyChangeSupport.firePropertyChange("topicConfigMap", null, null);
        }

        this.propertyChangeSupport.firePropertyChange("unflushInterval", null, null);
    }

    /**
     * 添加一个topic配置
     * @param topic         添加topic
     * @param topicConfig   topic配置对象
     */
    public void addTopic(String topic, TopicConfig topicConfig) {
        this.topics.add(topic);
        this.topicConfigMap.put(topic, topicConfig);
        this.propertyChangeSupport.firePropertyChange("topics", null, null);
        this.propertyChangeSupport.firePropertyChange("topicConfigMap", null, null);
    }

    /**
     * 从配置文件获取配置参数
     * @param section       表示配置文件中的配置
     * @param key           参数名
     * @param defaultValue  默认值，如果没有配置则返回默认值
     * @return
     */
    private int getInt(final Section section, final String key, final int defaultValue) {
        final String value = section.get(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        } else {
            final Long rt = (Long) AviatorEvaluator.execute(value);
            return rt.intValue();
        }
    }

    /**
     * 从配置文件获取配置参数
     * @param section       表示配置文件中的配置
     * @param key           参数名
     * @return
     */
    private int getInt(final Section section, final String key) {
        final String value = section.get(key);
        if (StringUtils.isBlank(value)) {
            throw new NullPointerException("Blank value for " + key);
        } else {
            final Long rt = (Long) AviatorEvaluator.execute(value);
            return rt.intValue();
        }
    }

    /**
     * 从配置文件获取配置参数
     * @param section       表示配置文件中的配置
     * @param key           参数名
     * @return
     */
    private boolean getBoolean(final Section section, final String key) {
        final String value = section.get(key);
        if (StringUtils.isBlank(value)) {
            throw new NullPointerException("Blank value for " + key);
        } else {
            final Boolean rt = (Boolean) AviatorEvaluator.execute(value);
            return rt;
        }
    }

    /**
     * 从配置文件获取配置参数
     * @param section       表示配置文件中的配置
     * @param key           参数名
     * @return
     */
    private long getLong(final Section section, final String key) {
        final String value = section.get(key);
        if (StringUtils.isBlank(value)) {
            throw new NullPointerException("Blank value for " + key);
        } else {
            final Long rt = (Long) AviatorEvaluator.execute(value);
            return rt.longValue();
        }
    }

    /**
     * 重新加载topics配置到内存，并发送参数变更通知，通过{@link PropertyChangeSupport}发送变更通知
     */
    @Override
    public void reload() {
        final File file = new File(this.path);
        if (file.lastModified() != this.lastModified) {
            try {
                log.info("Reloading topics......");
                final Ini conf = this.createIni(file);
                MetaConfig.this.populateTopicsConfig(conf);
                log.info("Reload topics successfully");
            } catch (final Exception e) {
                log.error("Reload config failed", e);
            }
        }
    }

    /**
     * 如果{@link this#zkConfig}为空的话，则创建一个 ZKConfig 对象
     */
    private void newZkConfigIfNull() {
        if (this.zkConfig == null) {
            this.zkConfig = new ZKConfig();
        }
    }

    /**
     * 校验配置是否正确
     */
    public void verify() {
        if (this.getTopics().isEmpty()) {
            throw new MetamorphosisServerStartupException("Empty topics list");
        }
        ZKConfig zkconfig = this.zkConfig;
        if (zkconfig == null) {
            throw new IllegalStateException("Null zookeeper config");
        }
        if (StringUtils.isBlank(this.zkConfig.zkConnect)) {
            throw new IllegalArgumentException("Empty zookeeper servers");
        }
    }

    /**
     * 关闭topic下的多个连续分区
     * @param topic topic
     * @param start 分区起始位置
     * @param end   分区截止位置
     */
    @Override
    public void closePartitions(final String topic, final int start, final int end) {
        if (StringUtils.isBlank(topic) || !this.topics.contains(topic)) {
            log.warn("topic=[" + topic + "]为空或未发布");
            return;
        }
        if (start < 0 || start > end) {
            log.warn("起始或结束的分区号非法,start=" + start + ",end=" + end);
            return;
        }

        for (int i = start; i <= end; i++) {
            this.closePartition(topic, i);
        }

    }

    /**
     * 关闭topic分区
     * @param topic     topic
     * @param partition 分区索引
     */
    private void closePartition(final String topic, final int partition) {
        Set<Integer> closedPartitions = this.closedPartitionMap.get(topic);
        if (closedPartitions == null) {
            closedPartitions = new HashSet<Integer>();
            this.closedPartitionMap.put(topic, closedPartitions);
        }
        if (closedPartitions.add(partition)) {
            log.info("close partition=" + partition + ",topic=" + topic);
        } else {
            log.info("partition=" + partition + " closed yet,topic=" + topic);
        }

    }

    /**
     * 判断分区是否关闭
     * @param topic     topic
     * @param partition 要关闭的分区索引
     * @return
     */
    public boolean isClosedPartition(final String topic, final int partition) {
        final Set<Integer> closedPartitions = this.closedPartitionMap.get(topic);
        return closedPartitions == null ? false : closedPartitions.contains(partition);
    }

    @Override
    public void openPartitions(final String topic) {
        final Set<Integer> partitions = this.closedPartitionMap.remove(topic);
        if (partitions == null || partitions.isEmpty()) {
            log.info("topic[" + topic + "] has no closed partitions");
        } else {
            log.info("open partitions " + partitions + ",topic=" + topic);
        }
    }

    /**
     * 根据topic获取topic配置
     * @param topic topic
     * @return
     */
    public final TopicConfig getTopicConfig(final String topic) {
        TopicConfig topicConfig = this.topicConfigMap.get(topic);
        if (topicConfig == null) {
            topicConfig = new TopicConfig(topic, this);
            TopicConfig old = this.topicConfigMap.putIfAbsent(topic, topicConfig);
            if (old != null) {
                topicConfig = old;
            }
        }
        return topicConfig;
    }






    // ------------------------
    // getter and setter ...
    // ------------------------

    public Map<String, TopicConfig> getTopicConfigMap() {
        return this.topicConfigMap;
    }

    public int getDashboardHttpPort() {
        return this.dashboardHttpPort;
    }
    public void setDashboardHttpPort(int dashboardHttpPort) {
        this.dashboardHttpPort = dashboardHttpPort;
    }

    public boolean isLoadMessageStoresInParallel() {
        return this.loadMessageStoresInParallel;
    }
    public void setLoadMessageStoresInParallel(boolean loadMessageStoresInParallel) {
        this.loadMessageStoresInParallel = loadMessageStoresInParallel;
    }

    public int getQuartzThreadCount() {
        return this.quartzThreadCount;
    }

    public boolean isAcceptPublish() {
        return this.acceptPublish;
    }

    public boolean isUpdateConsumerOffsets() {
        return this.updateConsumerOffsets;
    }
    public void setUpdateConsumerOffsets(boolean updateConsumerOffsets) {
        this.updateConsumerOffsets = updateConsumerOffsets;
    }

    public boolean isStat() {
        return this.stat;
    }
    public void setStat(boolean stat) {
        this.stat = stat;
    }

    public void setAcceptPublish(boolean acceptPublish) {
        this.acceptPublish = acceptPublish;
    }

    public boolean isAcceptSubscribe() {
        return this.acceptSubscribe;
    }
    public void setAcceptSubscribe(boolean acceptSubscribe) {
        this.acceptSubscribe = acceptSubscribe;
    }
    public SlaveConfig getSlaveConfig() {
        return this.slaveConfig;
    }

    public int getSlaveId() {
        return this.slaveConfig == null ? -1 : this.slaveConfig.getSlaveId();
    }
    public boolean isSlave() {
        return this.getSlaveId() >= 0;
    }

    public String getConfigFilePath() {
        return this.path;
    }
    public void setSlaveConfig(SlaveConfig slaveConfig) {
        this.slaveConfig = slaveConfig;
    }
    public void setQuartzThreadCount(final int quartzThreadCount) {
        this.quartzThreadCount = quartzThreadCount;
    }
    public int getMaxTxTimeoutTimerCapacity() {
        return this.maxTxTimeoutTimerCapacity;
    }

    public String getDeleteWhen() {
        return this.deleteWhen;
    }
    public void setDeleteWhen(final String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public void setMaxTxTimeoutTimerCapacity(final int maxTxTimeoutTimerCapacity) {
        this.maxTxTimeoutTimerCapacity = maxTxTimeoutTimerCapacity;
    }
    public int getMaxTxTimeoutInSeconds() {
        return this.maxTxTimeoutInSeconds;
    }
    public void setMaxTxTimeoutInSeconds(final int maxTxTimeoutInSeconds) {
        this.maxTxTimeoutInSeconds = maxTxTimeoutInSeconds;
    }

    public int getFlushTxLogAtCommit() {
        return this.flushTxLogAtCommit;
    }
    public void setFlushTxLogAtCommit(final int flushTxLogAtCommit) {
        this.flushTxLogAtCommit = flushTxLogAtCommit;
    }

    public int getMaxCheckpoints() {
        return this.maxCheckpoints;
    }
    public void setMaxCheckpoints(final int maxCheckpoints) {
        this.maxCheckpoints = maxCheckpoints;
    }

    public long getCheckpointInterval() {
        return this.checkpointInterval;
    }
    public void setCheckpointInterval(final long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public long getLastModified() {
        return this.lastModified;
    }

    public void addPropertyChangeListener(final String propertyName, final PropertyChangeListener listener) {
        this.propertyChangeSupport.addPropertyChangeListener(propertyName, listener);
    }
    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        this.propertyChangeSupport.removePropertyChangeListener(listener);
    }

    public long getConfigFileChecksum() {
        return this.configFileChecksum;
    }
    public void setConfigFileChecksum(long configFileChecksum) {
        this.configFileChecksum = configFileChecksum;
        this.propertyChangeSupport.firePropertyChange("configFileChecksum", null, null);
    }

    public String getDeletePolicy() {
        return this.deletePolicy;
    }
    public void setDeletePolicy(final String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }

    public List<String> getTopics() {
        return this.topics;
    }
    public void setTopics(final List<String> topics) {
        this.topics = topics;
    }

    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }
    public void setZkConfig(final ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
    }

    public int getNumPartitions() {
        return this.numPartitions;
    }
    public void setNumPartitions(final int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public int getBrokerId() {
        return this.brokerId;
    }
    public void setBrokerId(final int brokerId) {
        this.brokerId = brokerId;
    }

    public String getHostName() {
        return this.hostName;
    }
    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }

    public String getAppClassPath() {
        return this.appClassPath;
    }
    public void setAppClassPath(String appClassPath) {
        this.appClassPath = appClassPath;
    }

    public int getGetProcessThreadCount() {
        return this.getProcessThreadCount;
    }
    public void setGetProcessThreadCount(final int getProcessThreadCount) {
        this.getProcessThreadCount = getProcessThreadCount;
    }

    public int getPutProcessThreadCount() {
        return this.putProcessThreadCount;
    }
    public void setPutProcessThreadCount(final int putProcessThreadCount) {
        this.putProcessThreadCount = putProcessThreadCount;
    }

    public int getServerPort() {
        return this.serverPort;
    }
    public void setServerPort(final int serverPort) {
        this.serverPort = serverPort;
    }

    public int getMaxTransferSize() {
        return this.maxTransferSize;
    }
    public void setMaxTransferSize(final int maxTransferSize) {
        this.maxTransferSize = maxTransferSize;
    }

    public void setMaxSegmentSize(final int maxSegmentSize) {
        this.maxSegmentSize = maxSegmentSize;
    }
    public int getMaxSegmentSize() {
        return this.maxSegmentSize;
    }

    public int getUnflushInterval() {
        return this.unflushInterval;
    }
    public void setUnflushInterval(final int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }

    public void setUnflushThreshold(final int storeFlushThreshold) {
        this.unflushThreshold = storeFlushThreshold;
    }
    public int getUnflushThreshold() {
        return this.unflushThreshold;
    }

    public void setDataPath(final String dataPath) {
        final String oldDataPath = this.dataPath;
        this.dataPath = dataPath;
        // 如果dataLogPath没有改变过，那么也需要将dataLogPath指向新的dataPath
        if (oldDataPath.equals(this.dataLogPath)) {
            this.dataLogPath = this.dataPath;
        }
    }
    public String getDataPath() {
        return this.dataPath;
    }

    public String getDataLogPath() {
        return this.dataLogPath;
    }
    public void setDataLogPath(final String dataLogPath) {
        this.dataLogPath = dataLogPath;
    }


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}