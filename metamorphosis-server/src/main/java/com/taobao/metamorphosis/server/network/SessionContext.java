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
package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ConcurrentHashMap;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 会话上下文
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public interface SessionContext {

    /**
     * 获取所有的事务Id及对应的事务对象
     * @return
     */
    public ConcurrentHashMap<TransactionId, Transaction> getTransactions();

    /**
     * 获取当前线程的事务Id
     * @return
     */
    public String getSessionId();

    /**
     * 获取当前线程的连接对象
     * @return
     */
    public Connection getConnection();

    public boolean isInRecoverMode();

}