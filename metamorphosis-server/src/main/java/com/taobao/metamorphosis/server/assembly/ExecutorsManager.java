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
package com.taobao.metamorphosis.server.assembly;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.NamedThreadFactory;

/**
 * ���ڹ�����get��put������̳߳�
 */
public class ExecutorsManager implements Service {

    /** ���ڴ���get���󣨾��������ߴ�MQ��ȡ��Ϣ�����󣩵��̳߳� */
    ThreadPoolExecutor getExecutor;

    /** ���ڴ��������put���󣨾�����������MQ������Ϣ�����󣩵��̳߳� */
    ThreadPoolExecutor unOrderedPutExecutor;


    public ExecutorsManager(final MetaConfig metaConfig) {
        super();
        this.getExecutor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getGetProcessThreadCount(),
                    new NamedThreadFactory("GetProcess"));
        this.unOrderedPutExecutor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getPutProcessThreadCount(),
                    new NamedThreadFactory("PutProcess"));
    }

    @Override
    public void init() {
    }

    @Override
    public void dispose() {
        if (this.getExecutor != null) {
            this.getExecutor.shutdown();
        }

        if (this.unOrderedPutExecutor != null) {
            this.unOrderedPutExecutor.shutdown();
        }
        try {
            // shutdown�����̳߳�״̬��ΪSHUTDOWN,����������ֹͣ�����ø÷�����ֹͣ�����ⲿsubmit�����񣬲����ڲ������ܵ�����Ͷ�����ȴ�������ִ����󣬲�����ֹͣ
            // awaitTermination����ǰ�߳�������ֱ�����������ύ�����񣨰��������ܵĺͶ����еȴ��ģ�ִ������ߵȳ�ʱʱ�䵽
            this.getExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            this.unOrderedPutExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            // ignore
        }
    }

    public ThreadPoolExecutor getGetExecutor() {
        return this.getExecutor;
    }

    public ThreadPoolExecutor getUnOrderedPutExecutor() {
        return this.unOrderedPutExecutor;
    }


}