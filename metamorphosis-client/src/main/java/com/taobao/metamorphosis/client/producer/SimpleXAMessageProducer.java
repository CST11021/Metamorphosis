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
package com.taobao.metamorphosis.client.producer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.transaction.xa.XAResource;

import org.apache.commons.lang.StringUtils;

import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper.BrokerChangeListener;
import com.taobao.metamorphosis.client.transaction.TransactionContext;
import com.taobao.metamorphosis.exception.InvalidBrokerException;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * XA消息生产者的实现类
 * 
 * @author boyan
 * 
 */
public class SimpleXAMessageProducer extends SimpleMessageProducer implements XAMessageProducer, BrokerChangeListener {

    private String uniqueQualifier = DEFAULT_UNIQUE_QUALIFIER_PREFIX + "-" + getLocalhostName();

    private static final String OVERWRITE_HOSTNAME_SYSTEM_PROPERTY = "metaq.client.xaproducer.hostname";

    /** 保存该XA生产者发布的topic */
    final Set<String> publishedTopics = new ConcurrentHashSet<String>();

    /** 用于随机从{@link #urls}中选取一个broker */
    private final Random rand = new Random();

    /** 保存broker的url，这些broker都含有相同的topic */
    private volatile String[] urls;


    public SimpleXAMessageProducer(final MetaMessageSessionFactory messageSessionFactory,
                                   final RemotingClientWrapper remotingClient,
                                   final PartitionSelector partitionSelector,
                                   final ProducerZooKeeper producerZooKeeper,
                                   final String sessionId) {
        super(messageSessionFactory, remotingClient, partitionSelector, producerZooKeeper, sessionId);
    }


    public static String getLocalhostName() {
        String property = System.getProperty(OVERWRITE_HOSTNAME_SYSTEM_PROPERTY);
        if (property != null && property.trim().length() > 0) {
            return property;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (final UnknownHostException e) {
            throw new RuntimeException("unable to retrieve localhost name");
        }
    }

    @Override
    public void publish(final String topic) {
        super.publish(topic);
        if (this.publishedTopics.add(topic)) {
            // try to select a broker that contains those topics.
            this.generateTransactionBrokerURLs();
        }
    }

    @Override
    public void brokersChanged(String topic) {
        this.generateTransactionBrokerURLs();
    }

    private void generateTransactionBrokerURLs() {
        // 获取topic
        final List<Set<String>> brokerUrls = new ArrayList<Set<String>>();
        for (final String topic : this.publishedTopics) {
            brokerUrls.add(this.producerZooKeeper.getServerUrlSetByTopic(topic));
            // Listen for brokers changing.
            this.producerZooKeeper.onBrokerChange(topic, this);
        }

        // 获取topic分布在每个broker的那些broker
        final Set<String> resultSet = intersect(brokerUrls);
        if (resultSet.isEmpty()) {
            throw new InvalidBrokerException("Could not select a common broker url for  topics:" + this.publishedTopics);
        }
        String[] newUrls = resultSet.toArray(new String[resultSet.size()]);
        Arrays.sort(newUrls);
        // Set new urls array.
        this.urls = newUrls;
    }

    private String selectTransactionBrokerURL() {
        String[] copiedUrls = this.urls;
        if (copiedUrls == null || copiedUrls.length == 0) {
            throw new InvalidBrokerException("Could not select a common broker url for  topics:" + this.publishedTopics);
        }
        return copiedUrls[this.rand.nextInt(copiedUrls.length)];
    }

    /**
     * 获取List中每个Set里都相同的元素，例如：[1, 3, 5], [2, 3, 4], [3, 6, 9]三个set，则返回[3]
     *
     * @param sets
     * @param <T>
     * @return
     */
    static <T> Set<T> intersect(final List<Set<T>> sets) {
        if (sets == null || sets.size() == 0) {
            return null;
        }
        Set<T> rt = sets.get(0);
        for (int i = 1; i < sets.size(); i++) {
            final Set<T> copy = new HashSet<T>(rt);
            copy.retainAll(sets.get(i));
            rt = copy;
        }
        return rt;
    }

    @Override
    public String getUniqueQualifier() {
        return this.uniqueQualifier;
    }

    @Override
    public void setUniqueQualifier(String uniqueQualifier) {
        this.checkUniqueQualifier(this.uniqueQualifier);
        this.uniqueQualifier = uniqueQualifier;
    }

    @Override
    public void setUniqueQualifierPrefix(String prefix) {
        this.checkUniqueQualifier(prefix);
        this.uniqueQualifier = prefix + "-" + getLocalhostName();
    }

    private void checkUniqueQualifier(String prefix) {
        if (StringUtils.isBlank(prefix)) {
            throw new IllegalArgumentException("Blank unique qualifier for SimpleXAMessageProducer");
        }
        if (StringUtils.containsAny(prefix, "\r\n\t: ")) {
            throw new IllegalArgumentException("Invalid unique qualifier,it should not contains newline,':' or blank characters.");
        }
    }

    @Override
    public XAResource getXAResource() throws MetaClientException {
        TransactionContext xares = this.transactionContext.get();
        if (xares != null) {
            return xares;
        } else {
            this.beginTransaction();
            xares = this.transactionContext.get();
            // 设置启用选定的broker
            String selectedServer = this.selectTransactionBrokerURL();
            xares.setServerUrl(selectedServer);
            xares.setUniqueQualifier(this.uniqueQualifier);
            xares.setXareresourceURLs(this.urls);
            // 指定发送的url
            this.logLastSentInfo(selectedServer);
            return xares;
        }
    }

    @Override
    public synchronized void shutdown() throws MetaClientException {
        for (String topic : this.publishedTopics) {
            this.producerZooKeeper.deregisterBrokerChangeListener(topic, this);
        }
        super.shutdown();
    }

}