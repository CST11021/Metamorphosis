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

import java.util.HashMap;
import java.util.Map;

import com.taobao.metamorphosis.utils.Config;


/**
 * 针对某个topic的特殊配置（不使用全局配置）
 * 
 * @author 无花,dennis
 * @since 2011-8-18 下午2:30:35
 */
// TODO 将其他针对某个topic的特殊配置项移到这里
public class TopicConfig extends Config {

    /** topic的名称 */
    private String topic;
    /** 每隔多少秒做一次磁盘sync，覆盖系统配置，可选 */
    private int unflushThreshold;
    /** 每隔多少条消息做一次磁盘sync，覆盖系统配置，可选 */
    private int unflushInterval;
    /** 表示该topic下的消息存储在磁盘的路径 */
    private String dataPath;
    /** 删除策略的执行时间，覆盖系统配置，可选。何时执行删除策略的cron表达式默认是0 0 6,18 * * ?，也就是每天的早晚6点执行处理策略 */
    private String deleteWhen;
    /** topic的删除策略，覆盖系统配置，可选 */
    private String deletePolicy;
    /** 该topic在本服务器的分区总数，覆盖系统配置，可选 */
    private int numPartitions;
    /** 是否接收该topic的消息，覆盖系统配置，可选 */
    private boolean acceptPublish = true;
    /** 是否接受消费者的订阅，覆盖系统配置，可选 */
    private boolean acceptSubscribe = true;
    /** 是否启用实时统计，启用则会在服务端对该topic的请求做实时统计，可以通过stats topic-name协议观察到该topic运行状况，可选 */
    private boolean stat;
    /** 将 "group." 开头的配置项保存到这里 */
    private Map<String/* group name */, String/* class name */> filterClassNames = new HashMap<String, String>();


    public TopicConfig(final String topic, final MetaConfig metaConfig) {
        this.topic = topic;
        this.unflushThreshold = metaConfig.getUnflushThreshold();
        this.unflushInterval = metaConfig.getUnflushInterval();
        this.dataPath = metaConfig.getDataPath();
        this.deleteWhen = metaConfig.getDeleteWhen();
        this.deletePolicy = metaConfig.getDeletePolicy();
        this.numPartitions = metaConfig.getNumPartitions();
        this.acceptPublish = metaConfig.isAcceptPublish();
        this.acceptSubscribe = metaConfig.isAcceptSubscribe();
        this.stat = metaConfig.isStat();
    }

    public TopicConfig(String topic,
                       int unflushThreshold,
                       int unflushInterval,
                       String dataPath,
                       String deleteWhen,
                       String deletePolicy,
                       int numPartitions,
                       boolean acceptPublish,
                       boolean acceptSubscribe,
                       boolean stat,
                       Map<String/* group name */, String/* class name */> filterClassNames) {
        super();
        this.topic = topic;
        this.unflushThreshold = unflushThreshold;
        this.unflushInterval = unflushInterval;
        this.dataPath = dataPath;
        this.deleteWhen = deleteWhen;
        this.deletePolicy = deletePolicy;
        this.numPartitions = numPartitions;
        this.acceptPublish = acceptPublish;
        this.acceptSubscribe = acceptSubscribe;
        this.stat = stat;
        this.filterClassNames = filterClassNames;
    }


    public final void addFilterClass(String group, String className) {
        this.filterClassNames.put(group, className);
    }

    public final String getFilterClass(String group) {
        return this.filterClassNames.get(group);
    }


    // --------------------
    // getter and setter ...
    // --------------------


    public boolean isAcceptPublish() {
        return this.acceptPublish;
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

    public int getNumPartitions() {
        return this.numPartitions;
    }
    public void setNumPartitions(final int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getDeletePolicy() {
        return this.deletePolicy;
    }
    public void setDeletePolicy(final String deletePolicy) {
        this.deletePolicy = deletePolicy;
    }

    public boolean isStat() {
        return this.stat;
    }
    public void setStat(boolean stat) {
        this.stat = stat;
    }

    public String getDeleteWhen() {
        return this.deleteWhen;
    }
    public void setDeleteWhen(final String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public String getDataPath() {
        return this.dataPath;
    }
    public void setDataPath(final String dataPath) {
        this.dataPath = dataPath;
    }

    public String getTopic() {
        return this.topic;
    }
    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public int getUnflushThreshold() {
        return this.unflushThreshold;
    }
    public void setUnflushThreshold(final int unflushThreshold) {
        this.unflushThreshold = unflushThreshold;
    }

    public int getUnflushInterval() {
        return this.unflushInterval;
    }
    public void setUnflushInterval(final int unflushInterval) {
        this.unflushInterval = unflushInterval;
    }

    @Override
    public TopicConfig clone() {
        return new TopicConfig(this.topic, this.unflushThreshold, this.unflushInterval, this.dataPath, this.deleteWhen,
                this.deletePolicy, this.numPartitions, this.acceptPublish, this.acceptSubscribe, this.stat,
                this.filterClassNames);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.acceptPublish ? 1231 : 1237);
        result = prime * result + (this.acceptSubscribe ? 1231 : 1237);
        result = prime * result + (this.dataPath == null ? 0 : this.dataPath.hashCode());
        result = prime * result + (this.deletePolicy == null ? 0 : this.deletePolicy.hashCode());
        result = prime * result + (this.deleteWhen == null ? 0 : this.deleteWhen.hashCode());
        result = prime * result + (this.filterClassNames == null ? 0 : this.filterClassNames.hashCode());
        result = prime * result + this.numPartitions;
        result = prime * result + (this.stat ? 1231 : 1237);
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        result = prime * result + this.unflushInterval;
        result = prime * result + this.unflushThreshold;
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
        TopicConfig other = (TopicConfig) obj;
        if (this.acceptPublish != other.acceptPublish) {
            return false;
        }
        if (this.acceptSubscribe != other.acceptSubscribe) {
            return false;
        }
        if (this.dataPath == null) {
            if (other.dataPath != null) {
                return false;
            }
        }
        else if (!this.dataPath.equals(other.dataPath)) {
            return false;
        }
        if (this.deletePolicy == null) {
            if (other.deletePolicy != null) {
                return false;
            }
        }
        else if (!this.deletePolicy.equals(other.deletePolicy)) {
            return false;
        }
        if (this.deleteWhen == null) {
            if (other.deleteWhen != null) {
                return false;
            }
        }
        else if (!this.deleteWhen.equals(other.deleteWhen)) {
            return false;
        }
        if (this.filterClassNames == null) {
            if (other.filterClassNames != null) {
                return false;
            }
        }
        else if (!this.filterClassNames.equals(other.filterClassNames)) {
            return false;
        }
        if (this.numPartitions != other.numPartitions) {
            return false;
        }
        if (this.stat != other.stat) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        }
        else if (!this.topic.equals(other.topic)) {
            return false;
        }
        if (this.unflushInterval != other.unflushInterval) {
            return false;
        }
        if (this.unflushThreshold != other.unflushThreshold) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TopicConfig [topic=" + this.topic + ", unflushThreshold=" + this.unflushThreshold
                + ", unflushInterval=" + this.unflushInterval + ", dataPath=" + this.dataPath + ", deleteWhen="
                + this.deleteWhen + ", deletePolicy=" + this.deletePolicy + ", numPartitions=" + this.numPartitions
                + ", acceptPublish=" + this.acceptPublish + ", acceptSubscribe=" + this.acceptSubscribe + ", stat="
                + this.stat + ", filterClassNames=" + this.filterClassNames + "]";
    }

}