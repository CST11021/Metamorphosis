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
package com.taobao.metamorphosis.server.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.server.network.SessionContext;


/**
 * 消息集合
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public interface MessageSet {

    /**
     * 切片
     *
     * @param offset    起始索引
     * @param limit     切片大小
     * @return
     * @throws IOException
     */
    public MessageSet slice(long offset, long limit) throws IOException;

    public void write(GetCommand getCommand, SessionContext ctx);

    /**
     * 将当个消息的数据追加到消息结合中
     *
     * @param buff      表示一个消息数据（这里的消息数据不单单是消息体的数据）
     * @return
     * @throws IOException
     */
    public long append(ByteBuffer buff) throws IOException;

    /**
     * 将消息写到磁盘
     *
     * @throws IOException
     */
    public void flush() throws IOException;

    public void read(final ByteBuffer bf, long offset) throws IOException;

    public void read(final ByteBuffer bf) throws IOException;

    public long getMessageCount();

}