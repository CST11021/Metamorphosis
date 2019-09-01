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
 * meta的协议处理接口，封装meta的核心逻辑
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public interface CommandProcessor extends Service {

    // ---------------
    // 命令相关的处理方法
    // ---------------

    /**
     * 客户端向MQ服务器发送消息时，服务端会调用该方法来处理put类型的请求
     *
     * @param request           put请求
     * @param sessionContext    session上下文
     * @param cb                回调接口
     * @throws Exception
     */
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext, final PutCallback cb) throws Exception;

    /**
     * 客户端从MQ服务器拉取消息时，服务端会调用该方法来处理get类型的请求
     *
     * @param request           get请求
     * @param ctx               session上下文
     * @return
     */
    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx);

    /** Under conditions that cannot use notify-remoting directly. */
    public ResponseCommand processGetCommand(GetCommand request, final SessionContext ctx, final boolean zeroCopy);

    /**
     * 客户端请求获取偏移量信息，客户端上传一个偏移量，然后MQ根据偏移量查找对应的偏移量地址
     *
     * @param request
     * @param ctx
     * @return 返回离指定offset往前追溯最近的可用offset ,当传入的offset超出范围的时候返回边界offset
     */
    public ResponseCommand processOffsetCommand(OffsetCommand request, final SessionContext ctx);

    public void processQuitCommand(QuitCommand request, final SessionContext ctx);

    public ResponseCommand processVesionCommand(VersionCommand request, final SessionContext ctx);

    public ResponseCommand processStatCommand(StatsCommand request, final SessionContext ctx);


    // -----------
    // 事务相关
    // -----------

    /**
     * 移除XA事务ID，当事务执行完成时，会调用该方法移除事务
     *
     * @param xid
     */
    public void removeTransaction(final XATransactionId xid);

    /**
     * 根据事务ID获取事务
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
     * 回滚事务
     *
     * @param context
     * @param xid
     * @throws Exception
     */
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception;

    /**
     * 提交事务
     *
     * @param context
     * @param xid
     * @param onePhase
     * @throws Exception
     */
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase) throws Exception;

    /**
     * 事务准备
     *
     * @param context
     * @param xid
     * @return
     * @throws Exception
     */
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception;

    /**
     * 开始事务
     *
     * @param context
     * @param xid
     * @param seconds
     * @throws Exception
     */
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds) throws Exception;

    /**
     * 获取本次请求的所有事务ID
     *
     * @param context
     * @param uniqueQualifier
     * @return
     * @throws Exception
     */
    public TransactionId[] getPreparedTransactions(final SessionContext context, String uniqueQualifier) throws Exception;

    // public void setTransactionTimeout(final SessionContext ctx, final TransactionId xid, int seconds) throws Exception;

}