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
 * �����߲���ֲ�ʽ����ļ����ӣ�����atomikos
 *
 * �й���atomikos�ֲ�ʽ������Բο���https://www.jianshu.com/p/86b4ab4f2d18
 *
 * XA��ɶ��
 * XA����X/Open��֯����ķֲ�ʽ����ļܹ������߽�Э�飩��XA�ܹ���Ҫ�����ˣ�ȫ�֣������������Transaction Manager���ͣ��ֲ�����Դ��������Resource Manager��֮��Ľӿڡ�XA�ӿ���˫���ϵͳ�ӿڣ��������������Transaction Manager���Լ�һ��������Դ��������Resource Manager��֮���γ�ͨ��������Ҳ����˵���ڻ���XA��һ�������У����ǿ�����Զ����Դ���������������һ��ϵͳ���ʶ�����ݿ⣬�򼴷������ݿ⡢�ַ�������Ϣ�м����������Դ���������Ǿ��ܹ�ʵ���ڶ�����ݿ����Ϣ�м��ֱ��ʵ��ȫ���ύ����ȫ��ȡ��������XA�淶����java�Ĺ淶������һ��ͨ�õĹ淶��
 * Ŀǰ�������ݿ⡢�Լ��ܶ���Ϣ�м����֧��XA�淶��
 * JTA������XA�淶�ġ�����Java�����Ĺ淶�����ԣ�������˵��ʹ��JTAʵ�ֲַ�ʽ�����ʱ����ʵ����˵��ʹ��JTA�淶��ʵ��ϵͳ�ڶ�����ݿ⡢��Ϣ�м������Դ������
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-26
 * 
 */
public class XATransactionProducer {

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

    public static void main(final String[] args) throws Exception {
        // create transaction manager,reuse it.
        final TransactionManager tm = new UserTransactionManager();

        final String topic = "meta-test";
        final XAMessageSessionFactory xasf = getXAMessageSessionFactory();
        // create XA producer,it should be used as a singleton instance.
        XAMessageProducer xaMessageProducer = xasf.createXAProducer();
        // publish topic
        xaMessageProducer.publish(topic);
        // create XA datasource,reuse it.
        final XADataSource xads = getXADataSource();

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;

        while ((line = readLine(reader)) != null) {
            final String address = line;
            try {
                final int uid = 100;

                // we should create a template every transaction.
                final XATransactionTemplate template = new XATransactionTemplate(tm, xads, xaMessageProducer);
                template.executeCallback(new XACallback() {
                    @Override
                    public Object execute(final Connection conn, final XAMessageProducer producer, Status status)
                            throws Exception {
                        final PreparedStatement pstmt =
                                conn.prepareStatement("insert into orders(uid,address) values(?,?)");
                        pstmt.setInt(1, uid);
                        pstmt.setString(2, null);
                        if (pstmt.executeUpdate() <= 0) {
                            status.setRollbackOnly();
                            return null;
                        }
                        pstmt.close();
                        if (!producer.sendMessage(new Message(topic, address.getBytes())).isSuccess()) {
                            status.setRollbackOnly();
                        }
                        return null;
                    }
                });

            }
            catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type message to send:");
        return reader.readLine();
    }

}