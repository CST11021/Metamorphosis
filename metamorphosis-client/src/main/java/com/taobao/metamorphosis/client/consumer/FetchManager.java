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
package com.taobao.metamorphosis.client.consumer;

/**
 *
 * Fetch请求管理器接口
 *
 * MetaQ的消费者是以pull模型来从服务端拉取数据并消费，该接口用于从MQ服务端抓取消息进行消费
 * 
 * @author boyan
 * @Date 2011-5-4
 * 
 */
public interface FetchManager {

    /**
     * 开始从MQ服务器Fetch消息
     */
    public void startFetchRunner();

    /**
     * 停止fetch
     * 
     * @throws InterruptedException
     */
    public void stopFetchRunner() throws InterruptedException;

    /**
     * 重设状态，重设状态后可重用并start
     */
    public void resetFetchState();

    /**
     * 获取当前抓取请求的个数
     *
     * @since 1.4.4
     * @return
     */
    public int getFetchRequestCount();

    /**
     * 添加一个fetch请求对象到队列中
     * 
     * @param request
     */
    public void addFetchRequest(FetchRequest request);

    /**
     * 是否关闭
     * 
     * @return
     */
    public boolean isShutdown();

}