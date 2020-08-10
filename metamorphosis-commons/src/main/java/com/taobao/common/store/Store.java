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
 * MQͨ���ýӿڽ���Ϣ���浽ָ���ķ�����ÿ��������Ӧһ��Storeʵ������Store�ӿ�������ʵ�֣��ֱ��ǣ�
 * JournalStore��ͨ����־�ļ�ʵ�ֵ�key/value�ԵĴ洢
 * MessageStore���̳���JournalStore����֤�Ƚ��ȳ�˳���JournalStore
 * MemStore������ConcurrentHashMapʵ��key/value�Ĵ洢
 *
 *
 * @author boyan
 * @since 1.0, 2009-12-11 ����11:17:22
 */
public interface Store {

    /**
     * ������Ϣ
     *
     * @param key       ��ʾ��Ϣ��id��ͨ��{@link IdWorker}����
     * @param data      ��ʾ��Ϣ���󣬶�Ӧ{@link Message}
     * @throws IOException
     */
    void add(byte[] key, byte[] data) throws IOException;

    /**
     * ������Ϣ
     *
     * @param key       ��ʾ��Ϣ��id��ͨ��{@link IdWorker}����
     * @param data      ��ʾ��Ϣ���󣬶�Ӧ{@link Message}
     * @param force     ��ʾ�Ƿ�ͬ��д��
     * @throws IOException
     */
    void add(byte[] key, byte[] data, boolean force) throws IOException;

    /**
     * ����Ϣ�洢�����Ƴ���Ϣ
     *
     * @param key       ��ʾ��Ϣ��id��ͨ��{@link IdWorker}����
     * @return
     * @throws IOException
     */
    boolean remove(byte[] key) throws IOException;

    /**
     * ����Ϣ�洢�����Ƴ���Ϣ
     *
     * @param key       ��ʾ��Ϣ��id��ͨ��{@link IdWorker}����
     * @param force     ��ʾ�Ƿ�ʹ��ͬ��ɾ���ķ�ʽ
     * @return
     * @throws IOException
     */
    boolean remove(byte[] key, boolean force) throws IOException;

    /**
     * ������Ϣid��ȡ��Ϣ
     *
     * @param key       ��ʾ��Ϣ��id��ͨ��{@link IdWorker}����
     * @return
     * @throws IOException
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * ���½ӿڣ����ⲿ���ã�һ����Ϣ���ᱻ�޸ģ�ֻ�Ǹô洢���ṩ���µ�����
     *
     * @param key
     * @param data
     * @return
     * @throws IOException
     */
    boolean update(byte[] key, byte[] data) throws IOException;

    /**
     * ���ص�ǰ��Ϣ�洢����Ϣ��������MQ������յ����������ߵ���Ϣʱ���ȱ��浽�洢������ʱsize + 1������Ϣ�Ӵ洢��ͬ�������̺�size - 1
     *
     * @return
     */
    int size();

    public long getMaxFileCount();

    public void setMaxFileCount(long maxFileCount);

    /**
     * ���ر�����Ϣid�ĵ�����
     *
     * @return
     * @throws IOException
     */
    Iterator<byte[]> iterator() throws IOException;

    /**
     * �ر���Ϣ�洢������
     *
     * @throws IOException
     */
    void close() throws IOException;

}