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
 * MateQ������������
 *
 * @author boyan
 * @author wuhua
 * @Date 2011-4-21
 */
public class MetaConfig extends Config implements Serializable, MetaConfigMBean {

    static final Log log = LogFactory.getLog(MetaConfig.class);

    static final long serialVersionUID = -1L;

    /** ��������Ⱥ��Ψһ��id������Ϊ����0-1024֮�䡣�Է�������Ⱥ�Ķ�����ʹ��ͬһ��zookeeper������zookeeper�ϵ�root path��ͬ������μ�zookeeper���� */
    private int brokerId = 0;
    /** �û�Ŀ¼�µ�metaĿ¼, ��ʾ��Ϣ�洢��MQ�������ϵĴ���Ŀ¼ */
    private String dataPath = System.getProperty("user.home") + File.separator + "meta";
    /** MQĬ�ϵĶ˿� */
    private int serverPort = 8123;
    /** ���ư�˿� */
    private int dashboardHttpPort = 8120;
    /** ��ʾMQ���������ڵĻ���, ���û�����ã�Ĭ��Ϊ����IP��ַ */
    private String hostName;
    /**
     * topic��������ÿ��topic��MQ�������Ͽ����ж������������������MQ������������Ϣʱ������ͻ���û��ָ���ķ������ͻ���û��ָ������ʱ��
     * �����еķ�������Ĭ����-1�������������ӿ��ã������ǿ��Ա��رյģ��ķ����У����ѡ��һ��������������Ϣ
     */
    private int numPartitions = 1;
    /**
     * ÿ����������Ϣ��һ�δ���ͬ����ǿ�ƽ����ĵ�����ˢ����̡�Ĭ��Ϊ1000����Ҳ����˵�ڵ�������£��������ʧ1000����Ϣ��
     * ������Ϊ1��ǿ��ÿ��д������ͬ�������̡���<=0������£����������Զ�����group commit�������������Ϣ�ϲ���һ����ͬ��������IO���ܡ�
     * �������ԣ�group commit�������Ϣ�����ߵ�TPSû���ܵ�̫��Ӱ�죬���Ƿ���˵ĸ��ػ������ܶࡣ
     *
     * �ж��Ƿ������첽д�룺
     * 1���������ΪunflushThreshold <=0�����֣�����Ϊ�����첽д�룻
     * 2���������ΪunflushThreshold = 1������ͬ��д�룬��ÿд��һ����Ϣ�����ύ�����̣�
     * 3�����unflushThreshold > 0�������������ύ�����ǳ�ʱ�ύ
     *
     * �ͻ���ÿ��put��Ϣ��MQ������ʱ�����������ж��Ƿ�Ҫ������Ϣ����д����̣��жϹ������£�
     * ������topic����Ϣ�����д����̵�ʱ�� > ����ʱ�䣨unflushInterval�� ���� ���topic��ûд����̵���Ϣ���� > ���õ�������unflushThreshold����������д�����
     */
    private int unflushThreshold = 1000;
    /**
     * ������ٺ��붨����һ�δ���sync������Ϣ���浽���̣�,Ĭ����10�롣Ҳ����˵�ڷ�������������£���ඪʧ10���ڷ��͹�������Ϣ����������ΪС�ڻ��ߵ���0��
     * MessageStoreManager#FlushRunner�ᶨ�ڽ���Ϣ�������е���Ϣflush������
     */
    private int unflushInterval = 10000;
    /** ��ʾ������Ϣ�����ļ�������С��Ĭ��Ϊ1G��Ĭ�������޸Ĵ�ѡ�� */
    private int maxSegmentSize = 1 * 1024 * 1024 * 1024;
    /** ����������ߵ�������ݴ�С��Ĭ��Ϊ1M���������������Ϣ��С�������ã����̫С��ÿ���޷�����һ����������Ϣ�������ߣ���������������ͣ�͡������ó�һ��������ȡ������ */
    private int maxTransferSize = 1024 * 1024;
    /** getProcessThreadCount: ����get���󣨾��������ߴ�MQ��������ȡ��Ϣ�����󣩵Ĳ����߳�����Ĭ��ΪCPUS*10 */
    private int getProcessThreadCount = 10 * Runtime.getRuntime().availableProcessors();
    /** putProcessThreadCount: ����put���󣨾�����������MQ������������Ϣ�����󣩵Ĳ����߳�����Ĭ��ΪCPUS*10 */
    private int putProcessThreadCount = 10 * Runtime.getRuntime().availableProcessors();
    /**
     * �ļ�ɾ������:"��������,�趨ֵ�б�"��Ĭ��Ϊ����7��
     * deletePolicy: ����ɾ�����ԣ�Ĭ�ϳ���7�켴ɾ��,�����168��Сʱ��10s��ʾ10�룬10m��ʾ10���ӣ�10h��ʾ10Сʱ������ȷָ����λĬ��ΪСʱ��
     * delete��ָɾ��������ָ��ʱ��������ļ��������״Ӵ���ɾ����
     *
     * Ҳ����ѡ��archive���ԣ������Թ��ڵ������ļ���ɾ�����ǹ鵵����ʹ��archive���Ե�ʱ�����ѡ���Ƿ�ѹ�������ļ�����167,archive,true
     * ��ѡ�񽫸���ʱ�䳬��7��������ļ��鵵��ѹ��Ϊzip�ļ��������ѡ��ѹ������������Ϊ��չ��Ϊarc���ļ���
     */
    private String deletePolicy = "delete,168";
    /**
     * PropertyChangeSupport���������������������ֵ�仯��
     *      ��bean�����Է����仯ʱ��ʹ��PropertyChangeSupport�����firePropertyChange�������Ὣһ���¼����͸������Ѿ�ע��ļ�������
     *      �÷������������������Ե����֡��ɵ�ֵ�Լ��µ�ֵ�����Ե�ֵ�����Ƕ�������Ǽ��������ͣ��������а�װ��
     */
    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);
    /** ��ʾ�����ļ�������޸�ʱ�� */
    private long lastModified = -1;
    /** ��ʾ�����ļ��ľ���·�� */
    private volatile String path;
    /** App class path. */
    private String appClassPath;
    /**
     * ����������ã���󱣴��checkpoint��Ŀ����������̭���ϵ�
     * checkPoint���֪ʶ��
     * �����ݿ�ϵͳ�У�д��־��д�����ļ������ݿ���IO�����������ֲ������������ֲ�����д�����ļ����ڷ�ɢд��д��־�ļ���˳��д�����Ϊ�˱�
     * ֤���ݿ�����ܣ�ͨ�����ݿⶼ�Ǳ�֤���ύ(commit)���֮ǰҪ�ȱ�֤��־����д�뵽��־�ļ��У��������ݿ��򱣴������ݻ���(buffer cache)
     * ���ٲ����ڵķ���д�뵽�����ļ��С�Ҳ����˵��־д����ύ������ͬ���ģ�������д����ύ�����ǲ�ͬ���ġ������ʹ���һ�����⣬��һ������
     * �������ʱ�򲢲��ܱ�֤���������������ȫ��д�뵽�����ļ��У�������ʵ��������ʱ���Ҫʹ����־�ļ����лָ������������ݿ�ָ�������֮ǰ
     * ��״̬����֤���ݵ�һ���ԡ���������������е���Ҫ���ƣ�ͨ������ȷ�����ָ�ʱ��Щ������־Ӧ�ñ�ɨ�貢Ӧ���ڻָ���
     */
    private int maxCheckpoints = 3;
    /** �Զ�checkpoint�����Ĭ��1Сʱ */
    private long checkpointInterval = 60 * 60 * 1000L;
    /** �������ʱʱ�������Ĭ��3��� */
    private int maxTxTimeoutTimerCapacity = 30000;
    /** ������־ˢ�����ã�0��ʾ�ò���ϵͳ������1��ʾÿ��commit��ˢ�̣�2��ʾÿ��һ��ˢ��һ�� */
    private int flushTxLogAtCommit = 1;
    /** �������ʱʱ�䣬Ĭ��һ���� */
    private int maxTxTimeoutInSeconds = 60;
    /** ��־�洢Ŀ¼��Ĭ��ʹ��dataPath */
    private String dataLogPath = this.dataPath;
    /** ��ʱִ��ɾ�����Ե�cron���ʽ��Ĭ����0 0 6,18 * * ?��Ҳ����ÿ�������6��ִ�д�����ԡ�*/
    private String deleteWhen = "0 0 6,18 * * ?";
    /** quartzʹ�õ��̳߳ش�С */
    private int quartzThreadCount = 5;
    /** ��ʾ�����ļ���һ��У��ֵ��ֻҪ�ļ������б���ͻ᷵��һ���µ���ֵ */
    private long configFileChecksum;
    /** �Ƿ������Ϣ��Ĭ��Ϊtrue�����Ϊfalse���򲻻�ע�ᷢ����Ϣ��zookeeper�ϣ��ͻ��˵�Ȼ�޷�������Ϣ����broker�����������Ա�������topic���ø��ǡ�**/
    private boolean acceptPublish = true;
    /** ��acceptPublish���ƣ�Ĭ��ҲΪtrue�����Ϊfalse���򲻻�ע��������Ϣ��zookeeper�ϣ��������޷����ָ�broker����Ȼ�޷��Ӹ�broker������Ϣ�����������Ա�������topic���ø��ǡ�*/
    private boolean acceptSubscribe = true;
    /** ȫ���Եؿ����Ƿ���ʵʱͳ�ƣ��ɱ�topic���ø��ǣ�Ĭ��Ϊfalse */
    private boolean stat;
    /** �������ߵ�offset����Broker�����ݷ�Χ�ڣ��Ƿ�ǿ�Ƹ��������ߵ�offsetΪ��ǰ���offset��Ĭ��Ϊfalse�����Կ����������鿪����ѡ��������������顣 */
    private boolean updateConsumerOffsets = Boolean.parseBoolean(System.getProperty("meta.get.tellMaxOffset", "false"));
    /** �Ƿ��м�����Ϣ�洢��MQ�������ϵ���Ϣ�Ǳ��洢�ڴ��̣������Ա�ʶ�Ƿ�����ʱ���м������ݣ����������������ٶȡ�Ĭ�ϲ�������������������־˳��������� */
    private boolean loadMessageStoresInParallel = false;
    /** ���ڱ��������ļ������õ�topic */
    private List<String> topics = new ArrayList<String>();
    /** ���������ļ������õ�topic����Ӧ��TopicConfig��Map<topic, TopicConfig>*/
    private ConcurrentHashMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<String, TopicConfig>();
    /** Async slave config */
    private SlaveConfig slaveConfig;
    /** zk���� */
    private ZKConfig zkConfig;
    /** Map<topic, partition> ��ʾtopic�͹رյķ��� */
    private final Map<String, Set<Integer>> closedPartitionMap = new CopyOnWriteMap<String, Set<Integer>>();


    /** ������ */
    public MetaConfig() {
        super();
        // ������ע�ᵽjava��MBeanServer
        MetaMBeanServer.registMBean(this, null);
    }


    /**
     * ���������ļ�
     * @param path �����ļ�·��
     */
    public void loadFromFile(final String path) {
        try {
            this.path = path;
            final File file = new File(path);

            // ��ʾ�����ļ���Ŀ¼��providedĿ¼
            File appClassDir = new File(file.getParentFile().getParentFile(), "provided");
            if (appClassDir.exists() && appClassDir.isDirectory()) {
                // It's a directory,it must be ends with "/"
                this.appClassPath = appClassDir.getAbsolutePath() + "/";
            }

            if (!file.exists()) {
                throw new MetamorphosisServerStartupException("File " + path + " is not exists");
            }

            // ��.ini�������ļ�תΪIni����
            final Ini conf = this.createIni(file);
            // �����ļ�����
            this.populateAttributes(conf);
        } catch (final IOException e) {
            throw new MetamorphosisServerStartupException("Parse configuration failed,path=" + path, e);
        }
    }

    /**
     * ���ַ����м�������
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
     * ����.ini�������ļ�תΪ��{@link Ini}����
     * @param file
     * @return
     * @throws IOException
     * @throws InvalidFileFormatException
     */
    private Ini createIni(final File file) throws IOException, InvalidFileFormatException {
        final Ini conf = new Ini(file);
        this.lastModified = file.lastModified();
        // CRC32:CRC�����ǡ�����У���롱����˼��CRC32���ʾ�����һ��32bit��8λʮ������������У��ֵ������CRC32����У��ֵʱԴ���ݿ��ÿһ
        // ��bit��λ���������˼��㣬�������ݿ��м�ʹֻ��һλ�����˱仯��Ҳ��õ���ͬ��CRC32ֵ.
        this.configFileChecksum = org.apache.commons.io.FileUtils.checksumCRC32(file);
        this.propertyChangeSupport.firePropertyChange("configFileChecksum", null, null);
        return conf;
    }

    /**
     * Ini��ʾһ��.ini�������ļ����÷������ڽ��������ļ�
     * @param conf
     */
    protected void populateAttributes(final Ini conf) {
        // ����[system]�µ�����
        this.populateSystemConf(conf);
        // ����[zookeeper]�µ�����
        this.populateZookeeperConfig(conf);
        // ����topic��ص�����
        this.populateTopicsConfig(conf);
    }

    /**
     * ����ϵͳ��[system]��ǩ�µģ���ص����ã����������е��������ã�
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
     * ����zookeeper��ص����ã����磺
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
     * ������ļ������ò���ͬ�����ڴ棬��ͨ��{@link PropertyChangeSupport}���Ʒ��ͱ��֪ͨ
     * @param conf
     */
    private void populateTopicsConfig(final Ini conf) {
        final Set<String> set = conf.keySet();

        // ���ڱ��������ļ������õ�topic
        final List<String> newTopics = new ArrayList<String>();
        // ���ڱ��������ļ���topic��Ӧ��������Ϣ��Map<topic, TopicConfig>
        final ConcurrentHashMap<String, TopicConfig> newTopicConfigMap = new ConcurrentHashMap<String, TopicConfig>();

        for (final String name : set) {
            // ����topic�������
            if (name != null && name.startsWith("topic=")) {
                final Section section = conf.get(name);
                final String topic = name.substring("topic=".length());

                final TopicConfig topicConfig = new TopicConfig(topic, this);

                // TopicConfig�ж����������
                Set<String> validKeySet = topicConfig.getFieldSet();
                Set<String> allKeySet = section.keySet();
                Set<String> filterClassKeys = new HashSet<String>();

                // �����ļ��ж����������
                Set<String> configKeySet = new HashSet<String>();

                // ���������ļ��е�������
                for (String key : allKeySet) {
                    if (key.startsWith("group.")) {
                        filterClassKeys.add(key);
                    } else {
                        configKeySet.add(key);
                    }
                }
                // У�������ļ��е���������û��д��
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
            // �������ò������֪ͨ
            this.propertyChangeSupport.firePropertyChange("topics", null, null);
            this.propertyChangeSupport.firePropertyChange("topicConfigMap", null, null);
        }

        this.propertyChangeSupport.firePropertyChange("unflushInterval", null, null);
    }

    /**
     * ���һ��topic����
     * @param topic         ���topic
     * @param topicConfig   topic���ö���
     */
    public void addTopic(String topic, TopicConfig topicConfig) {
        this.topics.add(topic);
        this.topicConfigMap.put(topic, topicConfig);
        this.propertyChangeSupport.firePropertyChange("topics", null, null);
        this.propertyChangeSupport.firePropertyChange("topicConfigMap", null, null);
    }

    /**
     * �������ļ���ȡ���ò���
     * @param section       ��ʾ�����ļ��е�����
     * @param key           ������
     * @param defaultValue  Ĭ��ֵ�����û�������򷵻�Ĭ��ֵ
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
     * �������ļ���ȡ���ò���
     * @param section       ��ʾ�����ļ��е�����
     * @param key           ������
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
     * �������ļ���ȡ���ò���
     * @param section       ��ʾ�����ļ��е�����
     * @param key           ������
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
     * �������ļ���ȡ���ò���
     * @param section       ��ʾ�����ļ��е�����
     * @param key           ������
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
     * ���¼���topics���õ��ڴ棬�����Ͳ������֪ͨ��ͨ��{@link PropertyChangeSupport}���ͱ��֪ͨ
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
     * ���{@link this#zkConfig}Ϊ�յĻ����򴴽�һ�� ZKConfig ����
     */
    private void newZkConfigIfNull() {
        if (this.zkConfig == null) {
            this.zkConfig = new ZKConfig();
        }
    }

    /**
     * У�������Ƿ���ȷ
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
     * �ر�topic�µĶ����������
     * @param topic topic
     * @param start ������ʼλ��
     * @param end   ������ֹλ��
     */
    @Override
    public void closePartitions(final String topic, final int start, final int end) {
        if (StringUtils.isBlank(topic) || !this.topics.contains(topic)) {
            log.warn("topic=[" + topic + "]Ϊ�ջ�δ����");
            return;
        }
        if (start < 0 || start > end) {
            log.warn("��ʼ������ķ����ŷǷ�,start=" + start + ",end=" + end);
            return;
        }

        for (int i = start; i <= end; i++) {
            this.closePartition(topic, i);
        }

    }

    /**
     * �ر�topic����
     * @param topic     topic
     * @param partition ��������
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
     * �жϷ����Ƿ�ر�
     * @param topic     topic
     * @param partition Ҫ�رյķ�������
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
     * ����topic��ȡtopic����
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
        // ���dataLogPathû�иı������ôҲ��Ҫ��dataLogPathָ���µ�dataPath
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