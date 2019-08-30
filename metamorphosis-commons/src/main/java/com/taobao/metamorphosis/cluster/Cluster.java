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
package com.taobao.metamorphosis.cluster;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表示Broker集群：
 *
 * 集群的部署方式主要有下面2种：
 *
 *      Broker Clusters 模式：实现负载均衡，多个broker之间同步消息，已达到服务器负载的可能。
 *      Master Slave 模式：实现高可用，当主服务器宕机时，备用服务器可以立即补充，以保证服务的继续。
 *
 * Master Slave只能实现高可用性，不能实现负载均衡。
 * Broker Cluster 只能实现负载均衡，不能实现高可用性。
 * Master Slave和Broker Cluster 结合使用可以实现高可用和负载均衡
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-28
 * 
 */
public class Cluster {

    /** 用于随机从master/slaver集群中获取一台broker */
    transient private final static Random random = new Random();

    /** Map<borkerId>, Set<Broker>> 这里的key为master的brokerId*/
    private final ConcurrentHashMap<Integer, Set<Broker>> brokers = new ConcurrentHashMap<Integer, Set<Broker>>();

    /** Map<brokerId, Set<Broker>> */
    public ConcurrentHashMap<Integer, Set<Broker>> getBrokers() {
        return this.brokers;
    }

    /**
     * 返回broker总数,包括master和slave
     *
     * @return
     */
    public int size() {
        int size = 0;
        for (Map.Entry<Integer/* broker id */, Set<Broker>> entry : this.brokers.entrySet()) {
            Set<Broker> brokers = entry.getValue();
            if (brokers != null) {
                size = size + brokers.size();
            }
        }
        return size;
    }

    /**
     * 根据master的brokerId，随机获取该master集群下的一个broker，优先返回master
     *
     * @param id
     * @return
     */
    public Broker getBrokerRandom(int id) {
        Set<Broker> set = this.brokers.get(id);
        if (set == null || set.size() <= 0) {
            return null;
        }
        if (set.size() == 1) {
            return (Broker) set.toArray()[0];
        }
        // prefer master.优先返回master
        for (Broker broker : set) {
            if (!broker.isSlave()) {
                return broker;
            }
        }
        return (Broker) set.toArray()[random.nextInt(set.size())];
    }

    /**
     * 获取master类型的broker
     *
     * @param id
     * @return
     */
    public Broker getMasterBroker(int id) {
        Set<Broker> set = this.brokers.get(id);
        if (set == null || set.size() <= 0) {
            return null;
        }
        for (Broker broker : set) {
            if (!broker.isSlave()) {
                return broker;
            }
        }
        return null;
    }

    /**
     * 添加一个broker到集群中
     *
     * @param id            表示master的brokerId
     * @param broker        表示slaver broker
     */
    public void addBroker(int id, Broker broker) {
        Set<Broker> set = this.brokers.get(id);
        if (set == null) {
            set = new HashSet<Broker>();
            this.brokers.put(id, set);
        }
        set.add(broker);
    }

    /**
     * 添加多个broker到集群中
     *
     * @param id            表示master的brokerId
     * @param brokers       表示slaver broker
     */
    public void addBroker(int id, Set<Broker> brokers) {
        Set<Broker> set = this.brokers.get(id);
        if (set == null) {
            set = new HashSet<Broker>();
            this.brokers.put(id, set);
        }
        set.addAll(brokers);
    }

    /**
     * 移除broker集群
     *
     * @param id        表示master的brokerId
     * @return 返回被移除的broker
     */
    public Set<Broker> remove(int id) {
        return this.brokers.remove(id);
    }

    /**
     * 获取master集群
     *
     * @return
     */
    public Cluster masterCluster() {
        Cluster cluster = new Cluster();
        for (Map.Entry<Integer, Set<Broker>> entry : this.brokers.entrySet()) {
            Set<Broker> set = entry.getValue();
            if (set == null || set.isEmpty()) {
                continue;
            }

            for (Broker broker : set) {
                if (broker != null && !broker.isSlave()) {
                    cluster.addBroker(entry.getKey(), broker);
                }
            }
        }
        return cluster;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Cluster) {
            Cluster other = (Cluster) obj;
            return this.brokers.equals(other.brokers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.brokers.hashCode();
    }
}