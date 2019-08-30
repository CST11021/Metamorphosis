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
 * ��ʾBroker��Ⱥ��
 *
 * ��Ⱥ�Ĳ���ʽ��Ҫ������2�֣�
 *
 *      Broker Clusters ģʽ��ʵ�ָ��ؾ��⣬���broker֮��ͬ����Ϣ���Ѵﵽ���������صĿ��ܡ�
 *      Master Slave ģʽ��ʵ�ָ߿��ã�����������崻�ʱ�����÷����������������䣬�Ա�֤����ļ�����
 *
 * Master Slaveֻ��ʵ�ָ߿����ԣ�����ʵ�ָ��ؾ��⡣
 * Broker Cluster ֻ��ʵ�ָ��ؾ��⣬����ʵ�ָ߿����ԡ�
 * Master Slave��Broker Cluster ���ʹ�ÿ���ʵ�ָ߿��ú͸��ؾ���
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-28
 * 
 */
public class Cluster {

    /** ���������master/slaver��Ⱥ�л�ȡһ̨broker */
    transient private final static Random random = new Random();

    /** Map<borkerId>, Set<Broker>> �����keyΪmaster��brokerId*/
    private final ConcurrentHashMap<Integer, Set<Broker>> brokers = new ConcurrentHashMap<Integer, Set<Broker>>();

    /** Map<brokerId, Set<Broker>> */
    public ConcurrentHashMap<Integer, Set<Broker>> getBrokers() {
        return this.brokers;
    }

    /**
     * ����broker����,����master��slave
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
     * ����master��brokerId�������ȡ��master��Ⱥ�µ�һ��broker�����ȷ���master
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
        // prefer master.���ȷ���master
        for (Broker broker : set) {
            if (!broker.isSlave()) {
                return broker;
            }
        }
        return (Broker) set.toArray()[random.nextInt(set.size())];
    }

    /**
     * ��ȡmaster���͵�broker
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
     * ���һ��broker����Ⱥ��
     *
     * @param id            ��ʾmaster��brokerId
     * @param broker        ��ʾslaver broker
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
     * ��Ӷ��broker����Ⱥ��
     *
     * @param id            ��ʾmaster��brokerId
     * @param brokers       ��ʾslaver broker
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
     * �Ƴ�broker��Ⱥ
     *
     * @param id        ��ʾmaster��brokerId
     * @return ���ر��Ƴ���broker
     */
    public Set<Broker> remove(int id) {
        return this.brokers.remove(id);
    }

    /**
     * ��ȡmaster��Ⱥ
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