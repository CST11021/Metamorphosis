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
package com.taobao.metamorphosis.server;

import javax.transaction.xa.XAException;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.exception.MetamorphosisException;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;


/**
 * meta��Э�鴦��ӿڣ���װmeta�ĺ����߼�
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public interface CommandProcessor extends Service {

    // ---------------
    // ������صĴ�����
    // ---------------

    /**
     * �ͻ�����MQ������������Ϣʱ������˻���ø÷���������put���͵�����
     *
     * @param request           put����
     * @param sessionContext    session������
     * @param cb                �ص��ӿ�
     * @throws Exception
     */
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb) throws Exception;

    /**
     * �ͻ��˴�MQ��������ȡ��Ϣʱ������˻���ø÷���������get���͵�����
     *
     * @param request           get����
     * @param ctx               session������
     * @return
     */
    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx);

    /** Under conditions that cannot use notify-remoting directly. */
    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx, final boolean zeroCopy);

    /**
     * �ͻ��������ȡƫ������Ϣ���ͻ����ϴ�һ��ƫ������Ȼ��MQ����ƫ�������Ҷ�Ӧ��ƫ������ַ
     *
     * @param request
     * @param ctx
     * @return ������ָ��offset��ǰ׷������Ŀ���offset ,�������offset������Χ��ʱ�򷵻ر߽�offset
     */
    public ResponseCommand processOffsetCommand(OffsetCommand request, final SessionContext ctx);

    public void processQuitCommand(QuitCommand request, final SessionContext ctx);

    public ResponseCommand processVesionCommand(VersionCommand request, final SessionContext ctx);

    public ResponseCommand processStatCommand(StatsCommand request, final SessionContext ctx);


    // -----------
    // �������
    // -----------

    /**
     * �Ƴ�XA����ID��������ִ�����ʱ������ø÷����Ƴ�����
     *
     * @param xid
     */
    public void removeTransaction(final XATransactionId xid);

    /**
     * ��������ID��ȡ����
     *
     * @param context
     * @param xid
     * @return
     * @throws MetamorphosisException
     * @throws XAException
     */
    public Transaction getTransaction(final SessionContext context, final TransactionId xid) throws MetamorphosisException, XAException;

    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception;

    /**
     * �ع�����
     *
     * @param context
     * @param xid
     * @throws Exception
     */
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception;

    /**
     * �ύ����
     *
     * @param context
     * @param xid
     * @param onePhase
     * @throws Exception
     */
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase) throws Exception;

    /**
     * ����׼��
     *
     * @param context
     * @param xid
     * @return
     * @throws Exception
     */
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception;

    /**
     * ��ʼ����
     *
     * @param context
     * @param xid
     * @param seconds
     * @throws Exception
     */
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds) throws Exception;

    /**
     * ��ȡ�����������������ID
     *
     * @param context
     * @param uniqueQualifier
     * @return
     * @throws Exception
     */
    public TransactionId[] getPreparedTransactions(final SessionContext context, String uniqueQualifier) throws Exception;

    // public void setTransactionTimeout(final SessionContext ctx, final TransactionId xid, int seconds) throws Exception;

}