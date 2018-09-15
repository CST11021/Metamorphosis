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
 * 用于管理处理get和put请求的线程池
 */
public class ExecutorsManager implements Service {

    /** 用于处理get请求（就是消费者从MQ拉取消息的请求）的线程池 */
    ThreadPoolExecutor getExecutor;

    /** 用于处理无序的put请求（就是生产者向MQ发送消息的请求）的线程池 */
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
            // shutdown：将线程池状态置为SHUTDOWN,并不会立即停止，调用该方法后，停止接收外部submit的任务，并且内部正在跑的任务和队列里等待的任务执行完后，才真正停止
            // awaitTermination：当前线程阻塞，直到等所有已提交的任务（包括正在跑的和队列中等待的）执行完或者等超时时间到
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