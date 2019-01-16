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

import java.io.IOException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.Shutdownable;


/**
 * 消费端的Recover管理器：当消息被过滤器拦截后会调用拒绝策略处理消息，此时会调用{@link #append(String, Message)}将消息保存到 RecoverManager 管理器，
 * RecoverManager会将消费失败的消息保存在客户端本地，当消费者启动的时候会重新加载本地的消息（之前消费失败的消息）进行重新消费这些消息。
 * 
 * @author 无花
 * @since 2011-10-31 下午3:40:04
 */

public interface RecoverManager extends Shutdownable {
    /**
     * 是否已经启动
     * 
     * @return
     */
    public boolean isStarted();

    /**
     * 启动recover，当消费者启动的时候会调用该方法，该方法将recover管理中消息进行重新消费
     * 
     * @param metaClientConfig
     */
    public void start(MetaClientConfig metaClientConfig);

    /**
     * 存入一个消息
     * 
     * @param group
     * @param message
     * @throws IOException
     */
    public void append(String group, Message message) throws IOException;
}