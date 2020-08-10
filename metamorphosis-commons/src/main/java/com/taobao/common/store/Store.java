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
 *   boyan <killme2008@gmail.com>
 */
package com.taobao.common.store;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.utils.IdWorker;

import java.io.IOException;
import java.util.Iterator;


/**
 * MQ通过该接口将消息保存到指定的分区，每个分区对应一个Store实例，该Store接口有三个实现，分别是：
 * JournalStore：通过日志文件实现的key/value对的存储
 * MessageStore：继承了JournalStore，保证先进先出顺序的JournalStore
 * MemStore：基于ConcurrentHashMap实现key/value的存储
 *
 *
 * @author boyan
 * @since 1.0, 2009-12-11 上午11:17:22
 */
public interface Store {

    /**
     * 保存消息
     *
     * @param key       表示消息的id，通过{@link IdWorker}生产
     * @param data      表示消息对象，对应{@link Message}
     * @throws IOException
     */
    void add(byte[] key, byte[] data) throws IOException;

    /**
     * 保存消息
     *
     * @param key       表示消息的id，通过{@link IdWorker}生产
     * @param data      表示消息对象，对应{@link Message}
     * @param force     表示是否同步写入
     * @throws IOException
     */
    void add(byte[] key, byte[] data, boolean force) throws IOException;

    /**
     * 从消息存储器里移除消息
     *
     * @param key       表示消息的id，通过{@link IdWorker}生产
     * @return
     * @throws IOException
     */
    boolean remove(byte[] key) throws IOException;

    /**
     * 从消息存储器里移除消息
     *
     * @param key       表示消息的id，通过{@link IdWorker}生产
     * @param force     表示是否使用同步删除的方式
     * @return
     * @throws IOException
     */
    boolean remove(byte[] key, boolean force) throws IOException;

    /**
     * 根据消息id获取消息
     *
     * @param key       表示消息的id，通过{@link IdWorker}生产
     * @return
     * @throws IOException
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * 更新接口：无外部调用，一般消息不会被修改，只是该存储器提供更新的能力
     *
     * @param key
     * @param data
     * @return
     * @throws IOException
     */
    boolean update(byte[] key, byte[] data) throws IOException;

    /**
     * 返回当前消息存储器消息个数，当MQ服务接收到来自生产者的消息时，先保存到存储器，此时size + 1，当消息从存储器同步到磁盘后size - 1
     *
     * @return
     */
    int size();

    public long getMaxFileCount();

    public void setMaxFileCount(long maxFileCount);

    /**
     * 返回遍历消息id的迭代器
     *
     * @return
     * @throws IOException
     */
    Iterator<byte[]> iterator() throws IOException;

    /**
     * 关闭消息存储管理器
     *
     * @throws IOException
     */
    void close() throws IOException;

}