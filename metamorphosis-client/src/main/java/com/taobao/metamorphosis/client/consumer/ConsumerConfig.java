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
 * 消费者配置，主要配置选项如下：
 * <ul>
 * <li>group:分组名称，必须，表示该消费者所在分组，同一分组的消费者正常情况下不会接收重复消息，共同消费某一topic</li>
 * <li>consumerId: 消费者id，用于唯一标识一个消费者，可不设置，系统会根据分组名称自动生成</li>
 * <li>commitOffsetPeriodInMills: 保存offset的时间间隔，默认5秒，单位毫秒</li>
 * <li>fetchTimeoutInMills: 同步获取消息的默认超时时间，默认10秒，单位毫秒</li>
 * <li>maxDelayFetchTimeInMills: 当获取消息失败的时候（包括get
 * miss或者任何异常情况)会延迟获取，此值设置最大的延迟时间，单位毫秒</li>
 * <li>fetchRunnerCount: 获取消息的线程数，默认cpu个。</li>
 * <li>partition:当使用直连模式时，此值指定连接的分区，形如"brokerId-partition"的字符串</li>
 * <li>offset:指定读取的offset偏移量,默认从0开始</li>
 * <li>maxFetchRetries:同一条消息在处理失败情况下最大重试次数，默认5次，超过就跳过这条消息并记录</li>
 * <li>maxIncreaseFetchDataRetries:拉取数据重试次数超过这个值,则增长每次拉取的数据量</li>
 * <li>loadBalanceStrategyType: 消费者负载均衡策略</li>
 * </ul>
 *
 *
 * group参数说明：
 *
 * 在MetaQ里，消费者被认为是一个集群，也就是说认为是有一组的机器在共同分担消费一个topic。因此消费者配置ConsumerConfig中最重要的配置是group，
 * 每个消费者都必须告诉MetaQ它属于哪个group，然后MetaQ会找出这个group下所有注册上来的消费者，在他们之间做负载均衡，共同消费一个或多个topic。
 * 注意，不同group之间可以认为是不同的消费者，他们消费同一个topic下的消息的进度是不同。
 *
 * 举例来说，假设你有一个topic为business-logs，是所有业务系统的日志。然后现在你对这些日志要做两个事情：一个是存储到HDFS这样的分布式文件
 * 系统，以便后续做分析处理；以个是Twitter Storm这样的实时分析系统，做实时的数据分析、告警和展现。显然，这里你就需要两个group，比如我们有
 * 一个group叫hdfs-writer，它有三台机器同时消费business-logs，将日志存储到HDFS集群。同时，你也有另一个group叫storm-spouts，有5台机器
 * 用来给storm集群喂数据。这两个group是隔离，虽然是消费同一个topic，但是两者是消费进度（消费了多少个消息，等待消费多少个消息等信息）是不同的。
 * 但是同一个group内，例如hdfs-writer的三台机器，这三台机器是共同消费business-logs下的消息，同一条消息只会被这hdfs-writer三台机器中的
 * 一台处理，但是这条消息还会被twitter-spouts等其他分组内的某一台机器消费。
 *
 *
 *
 *
 *
 *
 * maxDelayFetchTimeInMills参数说明：
 *
 * 当上一次没有抓取到的消息，抓取线程sleep的最大时间，默认5秒，单位毫秒。当某一次没有抓取到消息的时候，抓取线程会开始休眠maxDelayFetchTimeInMills
 * 的10分之1时间，如果下次还是没有抓到，则休眠maxDelayFetchTimeInMills的10分之2时间，以此类推直到最多休眠maxDelayFetchTimeInMills
 * 时间。中途如果任何一次抓取开始获取数据，则计数清零从10分之1重新开始计算。当你对消息的实时性特别敏感的时候应该调小此参数，并同时调小服务
 * 端的unflushInterval参数。
 *
 *
 * Offset存储说明：
 *
 * MetaQ的消费模型是一种拉取的模型，消费者根据上次消费数据的绝对偏移量(offset)从服务端的数据文件中拉取后面的数据继续消费，因此这个offset
 * 信息就非常关键，需要可靠地保存。默认情况下，MetaQ是将offset信息保存在你使用的zookeeper集群上，也就是ZkOffsetStorage所做的事情，它实
 * 现了OffsetStorage接口。通常这样的保存是可靠并且安全的，但是有时候可能你也需要其他选项，目前还提供两个不同的OffsetStorage实现：
 *
 * 1、LocalOffsetStorage，使用consumer的本地文件作为offset存储，默认存储在${HOME}/.meta_offsets的文件里。适合消费者分组只有一个消费者的
 * 情况，无需共享offset信息。例如广播类型的消费者就特别合适。
 *
 * 2、MysqlOffsetStorage，使用Mysql作为offset存储，使用前需要创建表结构：
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
 * 你也可以实现自己的OffsetStorage存储。如果你想使用除了zookeeper之外的offset存储，可以在创建消费者的时候传入：
 *
 *   consumer  sessionFactorycreateConsumer(consumerConfig, (dataSource));
 * mysql存储需要传入JDBC数据源。
 *
 *
 *
 * 第一次消费的offset初始值。前面提到ConsumerConfig有个offset参数可以设置第一次消费的时候开始的绝对偏移量，默认这个参数是0，也就是从服务
 * 端现有消息的最小偏移量开始，从头开始消费所有消息。但是，通常情况下，新的消费分组都是希望从最新的消息开始消费，ComsumerConfig提供了一个
 * setConsumeFromMaxOffset(boolean always)方法来设置从最新位置开始消费。其中always参数表示是否每次消费者启动都从最新位置开始消费，
 * 这样就忽略了在消费者停止期间的消息。通常仅在测试的时候将always参数设置为true，以便每次测试最新的消息。除非你真的不需要消费者停止期间（
 * 比如重启间隔）的消息，否则不要将always设置为真。
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

    /** MetaQ的消费者是以pull模型来从服务端拉取数据并消费，这个参数设置并行拉取的线程数，默认是CPU个数 */
    private int fetchRunnerCount = Runtime.getRuntime().availableProcessors();
    /** 当上一次没有抓取到的消息，抓取线程sleep的最大时间，默认5秒，单位毫秒 */
    private long maxDelayFetchTimeInMills = 5000;
    @Deprecated
    private long maxDelayFetchTimeWhenExceptionInMills = 10000;
    /** 同步抓取的请求超时，默认10秒，通常不需要修改此参数。 */
    private long fetchTimeoutInMills = 10000;

    /** 单个消费者的id，必须全局唯一，通常用于标识分组内的单个消费者，可不设置，系统会根据IP和时间戳自动生成 */
    private String consumerId;
    /** 表示消费端的消费分区，仅在直接连接服务器的时候有效 */
    private String partition;
    /** 第一次消费开始位置的offset，默认都是从服务端的最早数据开始消费 */
    private long offset = 0;
    /** 表示该消费者所在分组，同一分组的消费者正常情况下不会接收重复消息，共同消费某一topic */
    private String group;
    /**
     * 保存消费者已经消费的数据的offset的间隔时间，默认5秒，单位毫秒。更大的间隔，在故障和重启时间可能重复消费的消息更多，更小的间隔，
     * 可能给存储造成压力
     */
    private long commitOffsetPeriodInMills = 5000L;
    /** 同一条消息在处理失败情况下最大重试消费次数，默认5次，{@link #maxIncreaseFetchDataRetries}超过就跳过这条消息并调用RejectConsumptionHandler处理 */
    private int maxFetchRetries = 3;
    /** 设置每次订阅是否从最新位置开始消费,如果为true，表示每次启动都从最新位置开始消费,通常在测试的时候可以设置为true。*/
    private boolean alwaysConsumeFromMaxOffset = false;
    /** 消费端的负载均衡策略，这里使用默认的负载均衡策略，尽量使得负载在所有consumer之间平均分配，consumer之间分配的分区数差距不大于1 */
    private LoadBalanceStrategy.Type loadBalanceStrategyType = LoadBalanceStrategy.Type.DEFAULT;

    /** 把消息处理失败重试跟拉取数据失败重试分开,因为有时不需要处理失败重试(maxFetchRetries设为maxIntValue),但需要自增长拉取的数据量 */
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
     * 拉取数据重试次数超过这个值,则增长每次拉取的数据量
     * 
     * @return
     */
    public int getMaxIncreaseFetchDataRetries() {
        return this.maxIncreaseFetchDataRetries;
    }
    /**
     * 设置拉取数据重试次数超过这个值,则增长每次拉取的数据量
     * 
     * @param maxFetchRetriesForDataNotEnough
     */
    public void setMaxIncreaseFetchDataRetries(final int maxFetchRetriesForDataNotEnough) {
        this.maxIncreaseFetchDataRetries = maxFetchRetriesForDataNotEnough;
    }

    /**
     * 请求线程数，默认cpus个
     * 
     * @return
     */
    public int getFetchRunnerCount() {
        return this.fetchRunnerCount;
    }
    /**
     * 设置请求线程数，默认cpus个
     *
     * @param fetchRunnerCount
     */
    public void setFetchRunnerCount(final int fetchRunnerCount) {
        this.fetchRunnerCount = fetchRunnerCount;
    }

    /**
     * 请求offset起点
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }
    /**
     * 设置请求offset
     * 
     * @param offset
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }

    /**
     * 设置首次订阅是否从最新位置开始消费。
     */
    public void setConsumeFromMaxOffset() {
        this.setConsumeFromMaxOffset(false);
    }
    /**
     * 设置每次订阅是否从最新位置开始消费。
     * 
     * @since 1.4.5
     * @param always 如果为true，表示每次启动都从最新位置开始消费。通常在测试的时候可以设置为true。
     */
    public void setConsumeFromMaxOffset(boolean always) {
        this.alwaysConsumeFromMaxOffset = always;
        // 修正设置false 和预计不一致的问题
        if (always) {
            this.setOffset(Long.MAX_VALUE);
        } 
    }

    /**
     * 消费者分组名
     * 
     * @return
     */
    public String getGroup() {
        return this.group;
    }
    /**
     * 设置消费者分组名
     * 
     * @param group
     *            分组名，不得为空
     */
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * 获取分区，仅在直接连接服务器的时候有效
     * 
     * @return
     */
    public String getPartition() {
        return this.partition;
    }
    /**
     * 设置分区,仅在直接连接服务器的时候有效
     * 
     * @param partition 形如"brokerId-partition"的字符串
     */
    public void setPartition(final String partition) {
        this.partition = partition;
    }

    /**
     * 消费者id
     * 
     * @return
     */
    public String getConsumerId() {
        return this.consumerId;
    }
    /**
     * 设置消费者id，可不设置，系统将按照"ip_时间"的规则自动产生
     * 
     * @param consumerId
     */
    public void setConsumerId(final String consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * 请求超时时间，毫秒为单位，默认10秒
     * 
     * @return
     */
    public long getFetchTimeoutInMills() {
        return this.fetchTimeoutInMills;
    }
    /**
     * 设置请求超时时间，毫秒为单位，默认10秒
     * 
     * @param fetchTimeoutInMills
     *            毫秒
     */
    public void setFetchTimeoutInMills(final long fetchTimeoutInMills) {
        this.fetchTimeoutInMills = fetchTimeoutInMills;
    }

    /**
     * 请求间隔的最大时间，单位毫秒，默认5秒
     * 
     * @return
     */
    public long getMaxDelayFetchTimeInMills() {
        return this.maxDelayFetchTimeInMills;
    }
    /**
     * 设置请求间隔的最大时间，单位毫秒，默认5秒
     * 
     * @param maxDelayFetchTimeInMills
     */
    public void setMaxDelayFetchTimeInMills(final long maxDelayFetchTimeInMills) {
        this.maxDelayFetchTimeInMills = maxDelayFetchTimeInMills;
    }

    /**
     * 当请求发生异常时(例如无可用连接等),请求间隔的最大时间，单位毫秒，默认10秒
     * 
     * @deprecated 1.4开始废除，请使用maxDelayFetchTimeInMills
     * @return
     */
    @Deprecated
    public long getMaxDelayFetchTimeWhenExceptionInMills() {
        return this.maxDelayFetchTimeWhenExceptionInMills;
    }
    /**
     * 当请求发生异常时(例如无可用连接等),设置请求间隔的最大时间，单位毫秒，默认10秒
     * 
     * @deprecated 1.4开始废除，请使用maxDelayFetchTimeInMills
     * @param maxDelayFetchTimeWhenExceptionInMills
     */
    @Deprecated
    public void setMaxDelayFetchTimeWhenExceptionInMills(final long maxDelayFetchTimeWhenExceptionInMills) {
        this.maxDelayFetchTimeWhenExceptionInMills = maxDelayFetchTimeWhenExceptionInMills;
    }

    /**
     * 保存offset的间隔时间，单位毫秒，默认5秒
     * 
     * @return
     */
    public long getCommitOffsetPeriodInMills() {
        return this.commitOffsetPeriodInMills;
    }
    /**
     * 设置保存offset的间隔时间，单位毫秒，默认5秒
     * 
     * @param commitOffsetPeriodInMills
     *            毫秒
     */
    public void setCommitOffsetPeriodInMills(final long commitOffsetPeriodInMills) {
        this.commitOffsetPeriodInMills = commitOffsetPeriodInMills;
    }

    /**
     * 获取负载均衡策略类型
     * 
     * @return
     */
    public LoadBalanceStrategy.Type getLoadBalanceStrategyType() {
        return this.loadBalanceStrategyType;
    }
    /**
     * 设置负载均衡策略类型
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