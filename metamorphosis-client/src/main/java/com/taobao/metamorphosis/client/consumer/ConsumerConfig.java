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

import com.taobao.metamorphosis.client.MetaClientConfig;


/**
 * ���������ã���Ҫ����ѡ�����£�
 * <ul>
 * <li>group:�������ƣ����룬��ʾ�����������ڷ��飬ͬһ�������������������²�������ظ���Ϣ����ͬ����ĳһtopic</li>
 * <li>consumerId: ������id������Ψһ��ʶһ�������ߣ��ɲ����ã�ϵͳ����ݷ��������Զ�����</li>
 * <li>commitOffsetPeriodInMills: ����offset��ʱ������Ĭ��5�룬��λ����</li>
 * <li>fetchTimeoutInMills: ͬ����ȡ��Ϣ��Ĭ�ϳ�ʱʱ�䣬Ĭ��10�룬��λ����</li>
 * <li>maxDelayFetchTimeInMills: ����ȡ��Ϣʧ�ܵ�ʱ�򣨰���get
 * miss�����κ��쳣���)���ӳٻ�ȡ����ֵ���������ӳ�ʱ�䣬��λ����</li>
 * <li>fetchRunnerCount: ��ȡ��Ϣ���߳�����Ĭ��cpu����</li>
 * <li>partition:��ʹ��ֱ��ģʽʱ����ֵָ�����ӵķ���������"brokerId-partition"���ַ���</li>
 * <li>offset:ָ����ȡ��offsetƫ����,Ĭ�ϴ�0��ʼ</li>
 * <li>maxFetchRetries:ͬһ����Ϣ�ڴ���ʧ�������������Դ�����Ĭ��5�Σ�����������������Ϣ����¼</li>
 * <li>maxIncreaseFetchDataRetries:��ȡ�������Դ����������ֵ,������ÿ����ȡ��������</li>
 * <li>loadBalanceStrategyType: �����߸��ؾ������</li>
 * </ul>
 *
 *
 * group����˵����
 *
 * ��MetaQ������߱���Ϊ��һ����Ⱥ��Ҳ����˵��Ϊ����һ��Ļ����ڹ�ͬ�ֵ�����һ��topic���������������ConsumerConfig������Ҫ��������group��
 * ÿ�������߶��������MetaQ�������ĸ�group��Ȼ��MetaQ���ҳ����group������ע�������������ߣ�������֮�������ؾ��⣬��ͬ����һ������topic��
 * ע�⣬��ͬgroup֮�������Ϊ�ǲ�ͬ�������ߣ���������ͬһ��topic�µ���Ϣ�Ľ����ǲ�ͬ��
 *
 * ������˵����������һ��topicΪbusiness-logs��������ҵ��ϵͳ����־��Ȼ�����������Щ��־Ҫ���������飺һ���Ǵ洢��HDFS�����ķֲ�ʽ�ļ�
 * ϵͳ���Ա���������������Ը���Twitter Storm������ʵʱ����ϵͳ����ʵʱ�����ݷ������澯��չ�֡���Ȼ�����������Ҫ����group������������
 * һ��group��hdfs-writer��������̨����ͬʱ����business-logs������־�洢��HDFS��Ⱥ��ͬʱ����Ҳ����һ��group��storm-spouts����5̨����
 * ������storm��Ⱥι���ݡ�������group�Ǹ��룬��Ȼ������ͬһ��topic���������������ѽ��ȣ������˶��ٸ���Ϣ���ȴ����Ѷ��ٸ���Ϣ����Ϣ���ǲ�ͬ�ġ�
 * ����ͬһ��group�ڣ�����hdfs-writer����̨����������̨�����ǹ�ͬ����business-logs�µ���Ϣ��ͬһ����Ϣֻ�ᱻ��hdfs-writer��̨�����е�
 * һ̨��������������Ϣ���ᱻtwitter-spouts�����������ڵ�ĳһ̨�������ѡ�
 *
 *
 *
 *
 *
 *
 * maxDelayFetchTimeInMills����˵����
 *
 * ����һ��û��ץȡ������Ϣ��ץȡ�߳�sleep�����ʱ�䣬Ĭ��5�룬��λ���롣��ĳһ��û��ץȡ����Ϣ��ʱ��ץȡ�̻߳Ὺʼ����maxDelayFetchTimeInMills
 * ��10��֮1ʱ�䣬����´λ���û��ץ����������maxDelayFetchTimeInMills��10��֮2ʱ�䣬�Դ�����ֱ���������maxDelayFetchTimeInMills
 * ʱ�䡣��;����κ�һ��ץȡ��ʼ��ȡ���ݣ�����������10��֮1���¿�ʼ���㡣�������Ϣ��ʵʱ���ر����е�ʱ��Ӧ�õ�С�˲�������ͬʱ��С����
 * �˵�unflushInterval������
 *
 *
 * Offset�洢˵����
 *
 * MetaQ������ģ����һ����ȡ��ģ�ͣ������߸����ϴ��������ݵľ���ƫ����(offset)�ӷ���˵������ļ�����ȡ��������ݼ������ѣ�������offset
 * ��Ϣ�ͷǳ��ؼ�����Ҫ�ɿ��ر��档Ĭ������£�MetaQ�ǽ�offset��Ϣ��������ʹ�õ�zookeeper��Ⱥ�ϣ�Ҳ����ZkOffsetStorage���������飬��ʵ
 * ����OffsetStorage�ӿڡ�ͨ�������ı����ǿɿ����Ұ�ȫ�ģ�������ʱ�������Ҳ��Ҫ����ѡ�Ŀǰ���ṩ������ͬ��OffsetStorageʵ�֣�
 *
 * 1��LocalOffsetStorage��ʹ��consumer�ı����ļ���Ϊoffset�洢��Ĭ�ϴ洢��${HOME}/.meta_offsets���ļ���ʺ������߷���ֻ��һ�������ߵ�
 * ��������蹲��offset��Ϣ������㲥���͵������߾��ر���ʡ�
 *
 * 2��MysqlOffsetStorage��ʹ��Mysql��Ϊoffset�洢��ʹ��ǰ��Ҫ������ṹ��
 *
 * CREATE TABLE `meta_topic_partition_group_offset` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT,
 *   `topic` varchar(255) NOT NULL,
 *   `partition` varchar(255) NOT NULL,
 *   `group_id` varchar(255) NOT NULL,
 *   `offset` int(11) NOT NULL,
 *   `msg_id` int(11) NOT NULL,
 *   PRIMARY KEY (`id`),
 *   KEY `TOPIC_PART_GRP_IDX` (`topic`,`partition`,`group_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 * ��Ҳ����ʵ���Լ���OffsetStorage�洢���������ʹ�ó���zookeeper֮���offset�洢�������ڴ��������ߵ�ʱ���룺
 *
 *   consumer  sessionFactorycreateConsumer(consumerConfig, (dataSource));
 * mysql�洢��Ҫ����JDBC����Դ��
 *
 *
 *
 * ��һ�����ѵ�offset��ʼֵ��ǰ���ᵽConsumerConfig�и�offset�����������õ�һ�����ѵ�ʱ��ʼ�ľ���ƫ������Ĭ�����������0��Ҳ���Ǵӷ���
 * ��������Ϣ����Сƫ������ʼ����ͷ��ʼ����������Ϣ�����ǣ�ͨ������£��µ����ѷ��鶼��ϣ�������µ���Ϣ��ʼ���ѣ�ComsumerConfig�ṩ��һ��
 * setConsumeFromMaxOffset(boolean always)���������ô�����λ�ÿ�ʼ���ѡ�����always������ʾ�Ƿ�ÿ��������������������λ�ÿ�ʼ���ѣ�
 * �����ͺ�������������ֹͣ�ڼ����Ϣ��ͨ�����ڲ��Ե�ʱ��always��������Ϊtrue���Ա�ÿ�β������µ���Ϣ����������Ĳ���Ҫ������ֹͣ�ڼ䣨
 * �����������������Ϣ������Ҫ��always����Ϊ�档
 *
 *
 *
 * 
 * @author boyan
 * @Date 2011-4-28
 * @author wuhua
 * 
 */
public class ConsumerConfig extends MetaClientConfig {

    static final long serialVersionUID = -1L;

    /** MetaQ������������pullģ�����ӷ������ȡ���ݲ����ѣ�����������ò�����ȡ���߳�����Ĭ����CPU���� */
    private int fetchRunnerCount = Runtime.getRuntime().availableProcessors();
    /** ����һ��û��ץȡ������Ϣ��ץȡ�߳�sleep�����ʱ�䣬Ĭ��5�룬��λ���� */
    private long maxDelayFetchTimeInMills = 5000;
    @Deprecated
    private long maxDelayFetchTimeWhenExceptionInMills = 10000;
    /** ͬ��ץȡ������ʱ��Ĭ��10�룬ͨ������Ҫ�޸Ĵ˲����� */
    private long fetchTimeoutInMills = 10000;

    /** ���������ߵ�id������ȫ��Ψһ��ͨ�����ڱ�ʶ�����ڵĵ��������ߣ��ɲ����ã�ϵͳ�����IP��ʱ����Զ����� */
    private String consumerId;
    /** ��ʾ���Ѷ˵����ѷ���������ֱ�����ӷ�������ʱ����Ч */
    private String partition;
    /** ��һ�����ѿ�ʼλ�õ�offset��Ĭ�϶��Ǵӷ���˵��������ݿ�ʼ���� */
    private long offset = 0;
    /** ��ʾ�����������ڷ��飬ͬһ�������������������²�������ظ���Ϣ����ͬ����ĳһtopic */
    private String group;
    /**
     * �����������Ѿ����ѵ����ݵ�offset�ļ��ʱ�䣬Ĭ��5�룬��λ���롣����ļ�����ڹ��Ϻ�����ʱ������ظ����ѵ���Ϣ���࣬��С�ļ����
     * ���ܸ��洢���ѹ��
     */
    private long commitOffsetPeriodInMills = 5000L;
    /** ͬһ����Ϣ�ڴ���ʧ�����������������Ѵ�����Ĭ��5�Σ�{@link #maxIncreaseFetchDataRetries}����������������Ϣ������RejectConsumptionHandler���� */
    private int maxFetchRetries = 3;
    /** ����ÿ�ζ����Ƿ������λ�ÿ�ʼ����,���Ϊtrue����ʾÿ��������������λ�ÿ�ʼ����,ͨ���ڲ��Ե�ʱ���������Ϊtrue��*/
    private boolean alwaysConsumeFromMaxOffset = false;
    /** ���Ѷ˵ĸ��ؾ�����ԣ�����ʹ��Ĭ�ϵĸ��ؾ�����ԣ�����ʹ�ø���������consumer֮��ƽ�����䣬consumer֮�����ķ�������಻����1 */
    private LoadBalanceStrategy.Type loadBalanceStrategyType = LoadBalanceStrategy.Type.DEFAULT;

    /** ����Ϣ����ʧ�����Ը���ȡ����ʧ�����Էֿ�,��Ϊ��ʱ����Ҫ����ʧ������(maxFetchRetries��ΪmaxIntValue),����Ҫ��������ȡ�������� */
    private int maxIncreaseFetchDataRetries = 5;




    public ConsumerConfig(final String group) {
        super();
        this.group = group;
    }
    public ConsumerConfig(final String consumerId, final String group) {
        super();
        this.consumerId = consumerId;
        this.group = group;
    }
    public ConsumerConfig() {
        super();
    }




    public boolean isAlwaysConsumeFromMaxOffset() {
        return this.alwaysConsumeFromMaxOffset;
    }

    public int getMaxFetchRetries() {
        return this.maxFetchRetries;
    }
    public void setMaxFetchRetries(final int maxFetchRetries) {
        this.maxFetchRetries = maxFetchRetries;
    }

    /**
     * ��ȡ�������Դ����������ֵ,������ÿ����ȡ��������
     * 
     * @return
     */
    public int getMaxIncreaseFetchDataRetries() {
        return this.maxIncreaseFetchDataRetries;
    }
    /**
     * ������ȡ�������Դ����������ֵ,������ÿ����ȡ��������
     * 
     * @param maxFetchRetriesForDataNotEnough
     */
    public void setMaxIncreaseFetchDataRetries(final int maxFetchRetriesForDataNotEnough) {
        this.maxIncreaseFetchDataRetries = maxFetchRetriesForDataNotEnough;
    }

    /**
     * �����߳�����Ĭ��cpus��
     * 
     * @return
     */
    public int getFetchRunnerCount() {
        return this.fetchRunnerCount;
    }
    /**
     * ���������߳�����Ĭ��cpus��
     *
     * @param fetchRunnerCount
     */
    public void setFetchRunnerCount(final int fetchRunnerCount) {
        this.fetchRunnerCount = fetchRunnerCount;
    }

    /**
     * ����offset���
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }
    /**
     * ��������offset
     * 
     * @param offset
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }

    /**
     * �����״ζ����Ƿ������λ�ÿ�ʼ���ѡ�
     */
    public void setConsumeFromMaxOffset() {
        this.setConsumeFromMaxOffset(false);
    }
    /**
     * ����ÿ�ζ����Ƿ������λ�ÿ�ʼ���ѡ�
     * 
     * @since 1.4.5
     * @param always ���Ϊtrue����ʾÿ��������������λ�ÿ�ʼ���ѡ�ͨ���ڲ��Ե�ʱ���������Ϊtrue��
     */
    public void setConsumeFromMaxOffset(boolean always) {
        this.alwaysConsumeFromMaxOffset = always;
        // ��������false ��Ԥ�Ʋ�һ�µ�����
        if (always) {
            this.setOffset(Long.MAX_VALUE);
        } 
    }

    /**
     * �����߷�����
     * 
     * @return
     */
    public String getGroup() {
        return this.group;
    }
    /**
     * ���������߷�����
     * 
     * @param group
     *            ������������Ϊ��
     */
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * ��ȡ����������ֱ�����ӷ�������ʱ����Ч
     * 
     * @return
     */
    public String getPartition() {
        return this.partition;
    }
    /**
     * ���÷���,����ֱ�����ӷ�������ʱ����Ч
     * 
     * @param partition ����"brokerId-partition"���ַ���
     */
    public void setPartition(final String partition) {
        this.partition = partition;
    }

    /**
     * ������id
     * 
     * @return
     */
    public String getConsumerId() {
        return this.consumerId;
    }
    /**
     * ����������id���ɲ����ã�ϵͳ������"ip_ʱ��"�Ĺ����Զ�����
     * 
     * @param consumerId
     */
    public void setConsumerId(final String consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * ����ʱʱ�䣬����Ϊ��λ��Ĭ��10��
     * 
     * @return
     */
    public long getFetchTimeoutInMills() {
        return this.fetchTimeoutInMills;
    }
    /**
     * ��������ʱʱ�䣬����Ϊ��λ��Ĭ��10��
     * 
     * @param fetchTimeoutInMills
     *            ����
     */
    public void setFetchTimeoutInMills(final long fetchTimeoutInMills) {
        this.fetchTimeoutInMills = fetchTimeoutInMills;
    }

    /**
     * �����������ʱ�䣬��λ���룬Ĭ��5��
     * 
     * @return
     */
    public long getMaxDelayFetchTimeInMills() {
        return this.maxDelayFetchTimeInMills;
    }
    /**
     * ���������������ʱ�䣬��λ���룬Ĭ��5��
     * 
     * @param maxDelayFetchTimeInMills
     */
    public void setMaxDelayFetchTimeInMills(final long maxDelayFetchTimeInMills) {
        this.maxDelayFetchTimeInMills = maxDelayFetchTimeInMills;
    }

    /**
     * ���������쳣ʱ(�����޿������ӵ�),�����������ʱ�䣬��λ���룬Ĭ��10��
     * 
     * @deprecated 1.4��ʼ�ϳ�����ʹ��maxDelayFetchTimeInMills
     * @return
     */
    @Deprecated
    public long getMaxDelayFetchTimeWhenExceptionInMills() {
        return this.maxDelayFetchTimeWhenExceptionInMills;
    }
    /**
     * ���������쳣ʱ(�����޿������ӵ�),���������������ʱ�䣬��λ���룬Ĭ��10��
     * 
     * @deprecated 1.4��ʼ�ϳ�����ʹ��maxDelayFetchTimeInMills
     * @param maxDelayFetchTimeWhenExceptionInMills
     */
    @Deprecated
    public void setMaxDelayFetchTimeWhenExceptionInMills(final long maxDelayFetchTimeWhenExceptionInMills) {
        this.maxDelayFetchTimeWhenExceptionInMills = maxDelayFetchTimeWhenExceptionInMills;
    }

    /**
     * ����offset�ļ��ʱ�䣬��λ���룬Ĭ��5��
     * 
     * @return
     */
    public long getCommitOffsetPeriodInMills() {
        return this.commitOffsetPeriodInMills;
    }
    /**
     * ���ñ���offset�ļ��ʱ�䣬��λ���룬Ĭ��5��
     * 
     * @param commitOffsetPeriodInMills
     *            ����
     */
    public void setCommitOffsetPeriodInMills(final long commitOffsetPeriodInMills) {
        this.commitOffsetPeriodInMills = commitOffsetPeriodInMills;
    }

    /**
     * ��ȡ���ؾ����������
     * 
     * @return
     */
    public LoadBalanceStrategy.Type getLoadBalanceStrategyType() {
        return this.loadBalanceStrategyType;
    }
    /**
     * ���ø��ؾ����������
     * 
     * @param loadBalanceStrategyType
     */
    public void setLoadBalanceStrategyType(final LoadBalanceStrategy.Type loadBalanceStrategyType) {
        this.loadBalanceStrategyType = loadBalanceStrategyType;
    }



    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.alwaysConsumeFromMaxOffset ? 1231 : 1237);
        result = prime * result + (int) (this.commitOffsetPeriodInMills ^ this.commitOffsetPeriodInMills >>> 32);
        result = prime * result + (this.consumerId == null ? 0 : this.consumerId.hashCode());
        result = prime * result + this.fetchRunnerCount;
        result = prime * result + (int) (this.fetchTimeoutInMills ^ this.fetchTimeoutInMills >>> 32);
        result = prime * result + (this.group == null ? 0 : this.group.hashCode());
        result = prime * result + (this.loadBalanceStrategyType == null ? 0 : this.loadBalanceStrategyType.hashCode());
        result = prime * result + (int) (this.maxDelayFetchTimeInMills ^ this.maxDelayFetchTimeInMills >>> 32);
        result =
                prime
                * result
                + (int) (this.maxDelayFetchTimeWhenExceptionInMills ^ this.maxDelayFetchTimeWhenExceptionInMills >>> 32);
        result = prime * result + this.maxFetchRetries;
        result = prime * result + this.maxIncreaseFetchDataRetries;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + (this.partition == null ? 0 : this.partition.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        ConsumerConfig other = (ConsumerConfig) obj;
        if (this.alwaysConsumeFromMaxOffset != other.alwaysConsumeFromMaxOffset) {
            return false;
        }
        if (this.commitOffsetPeriodInMills != other.commitOffsetPeriodInMills) {
            return false;
        }
        if (this.consumerId == null) {
            if (other.consumerId != null) {
                return false;
            }
        }
        else if (!this.consumerId.equals(other.consumerId)) {
            return false;
        }
        if (this.fetchRunnerCount != other.fetchRunnerCount) {
            return false;
        }
        if (this.fetchTimeoutInMills != other.fetchTimeoutInMills) {
            return false;
        }
        if (this.group == null) {
            if (other.group != null) {
                return false;
            }
        }
        else if (!this.group.equals(other.group)) {
            return false;
        }
        if (this.loadBalanceStrategyType != other.loadBalanceStrategyType) {
            return false;
        }
        if (this.maxDelayFetchTimeInMills != other.maxDelayFetchTimeInMills) {
            return false;
        }
        if (this.maxDelayFetchTimeWhenExceptionInMills != other.maxDelayFetchTimeWhenExceptionInMills) {
            return false;
        }
        if (this.maxFetchRetries != other.maxFetchRetries) {
            return false;
        }
        if (this.maxIncreaseFetchDataRetries != other.maxIncreaseFetchDataRetries) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.partition == null) {
            if (other.partition != null) {
                return false;
            }
        }
        else if (!this.partition.equals(other.partition)) {
            return false;
        }
        return true;
    }

}