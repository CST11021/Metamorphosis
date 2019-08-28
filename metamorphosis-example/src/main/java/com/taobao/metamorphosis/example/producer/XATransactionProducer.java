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
package com.taobao.metamorphosis.example.producer;

import static com.taobao.metamorphosis.example.Help.initMetaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.XAMessageSessionFactory;
import com.taobao.metamorphosis.client.XAMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;
import com.taobao.metamorphosis.example.XACallback;
import com.taobao.metamorphosis.example.XATransactionTemplate;


/**
 * 发送者参与分布式事务的简单例子，基于atomikos
 *
 * 有关于atomikos分布式事务可以参考：https://www.jianshu.com/p/86b4ab4f2d18
 *
 * XA是啥？
 * XA是由X/Open组织提出的分布式事务的架构（或者叫协议）。XA架构主要定义了（全局）事务管理器（Transaction Manager）和（局部）资源管理器（Resource Manager）
 * 之间的接口。XA接口是双向的系统接口，在事务管理器（Transaction Manager）以及一个或多个资源管理器（Resource Manager）之间形成通信桥梁。
 * 也就是说，在基于XA的一个事务中，我们可以针对多个资源进行事务管理，例如一个系统访问多个数据库，或即访问数据库、又访问像消息中间件这样的资源。
 * 这样我们就能够实现在多个数据库和消息中间件直接实现全部提交、或全部取消的事务。XA规范不是java的规范，而是一种通用的规范，
 * 目前各种数据库、以及很多消息中间件都支持XA规范。
 * JTA是满足XA规范的、用于Java开发的规范。所以，当我们说，使用JTA实现分布式事务的时候，其实就是说，使用JTA规范，实现系统内多个数据库、消息中间件等资源的事务。
 *
 *
 * 所谓XA事务消息，是指在一个事务内除了MetaQ这个事务源之外，还有另外一个事务源参与了事务，最常见的比如数据库源。一个典型的场景是：
 * 往MetaQ发送消息，同时要向数据库插入一条数据，两个操作要么同时成功，要么同时失败。不允许出现发送消息成功，而插入数据库失败的情况，反之亦然。
 * 比如下订单这个操作，要往订单表插入一条记录，同时发送一条消息到MetaQ，执行一些异步任务如通知用户、物流，记录日志，统计分析等等，就需要分布式事务。
 * JavaEE规范支持XA协议，也就是两阶段提交协议，更详细的关于这块的信息请参考JTA规范和J2EE规范，阅读两阶段协议的相关资料来获得。
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-26
 * 
 */
public class XATransactionProducer {

    public static void main(final String[] args) throws Exception {
        // 1、创建一个分布式事务管理器
        final TransactionManager tm = new UserTransactionManager();

        // 2、创建消息会话工厂：一般会话工厂会使用单例来创建
        final XAMessageSessionFactory xasf = getXAMessageSessionFactory();

        // 3、创建消息生产者：一般使用单例来创建
        XAMessageProducer xaMessageProducer = xasf.createXAProducer();

        // 4、发布topic
        final String topic = "meta-test";
        xaMessageProducer.publish(topic);

        // 5、创建mysql数据源
        final XADataSource xads = getXADataSource();

        String line = null;
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while ((line = readLine(reader)) != null) {
            final String address = line;
            try {
                final int uid = 100;

                // 创建一个XA事务模板，we should create a template every transaction.
                final XATransactionTemplate template = new XATransactionTemplate(tm, xads, xaMessageProducer);
                template.executeCallback(new XACallback() {
                    @Override
                    public Object execute(final Connection conn, final XAMessageProducer producer, Status status) throws Exception {
                        // 1、保存订单
                        final PreparedStatement pstmt = conn.prepareStatement("insert into orders(uid,address) values(?,?)");
                        pstmt.setInt(1, uid);
                        pstmt.setString(2, null);
                        if (pstmt.executeUpdate() <= 0) {
                            status.setRollbackOnly();
                            return null;
                        }
                        pstmt.close();

                        // 2、发送消息
                        if (!producer.sendMessage(new Message(topic, address.getBytes())).isSuccess()) {
                            status.setRollbackOnly();
                        }
                        return null;
                    }
                });

            } catch (final Exception e) {
                e.printStackTrace();
            }
        }


    }

    private static XAMessageSessionFactory getXAMessageSessionFactory() throws Exception {
        return new XAMetaMessageSessionFactory(initMetaConfig());
    }

    private static XADataSource getXADataSource() throws SQLException {
        final MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
        mysqlXADataSource
                .setUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&connectTimeout=1000&autoReconnect=true");
        mysqlXADataSource.setUser("root");
        mysqlXADataSource.setPassword("");
        mysqlXADataSource.setPreparedStatementCacheSize(20);
        return mysqlXADataSource;
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type message to send:");
        return reader.readLine();
    }

}