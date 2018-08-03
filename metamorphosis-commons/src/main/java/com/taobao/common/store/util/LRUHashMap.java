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
package com.taobao.common.store.util;

import java.util.LinkedHashMap;


/**
 * LRU�㷨���
 *
 *  LRU��least recently used)�ǽ����������ʵ����ݸ���̭������ʵLRU����Ϊ�����ʹ�ù������ݣ���ô���������ʵĸ���Ҳ�࣬���û�б����ʣ�
 * ��ô���������ʵĸ���Ҳ�Ƚϵ͡�����ʵ�����������ȷ�ģ�������ΪLRU�㷨�򵥣��洢�ռ�û�б��˷ѣ����Ի����õıȽϹ㷺�ġ�
 *
 * @author dennis
 * 
 * @param <K>
 * @param <V>
 */
public class LRUHashMap<K, V> extends LinkedHashMap<K, V> {

    static final long serialVersionUID = 438971390573954L;

    /** HashMap������� */
    private final int maxCapacity;

    /** �Ƿ�����LRU�㷨 */
    private boolean enableLRU;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** ��ʾ�Ƴ����ϼ�ֵ��ʱ�Ĵ���ʱ�� */
    private transient EldestEntryHandler<K, V> handler;

    public LRUHashMap() {
        this(1000, true);
    }

    public LRUHashMap(final int maxCapacity, final boolean enableLRU) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = maxCapacity;
        this.enableLRU = enableLRU;
    }

    /**
     * ��put���µ�ֵʱ������÷�������trueʱ�����Ƴ���map�����ϵļ���ֵ��
     * @param eldest ��ʾ���ϵļ�ֵ��
     * @return
     */
    @Override
    protected boolean removeEldestEntry(final java.util.Map.Entry<K, V> eldest) {
        if (!this.enableLRU) {
            return false;
        }
        final boolean result = this.size() > maxCapacity;
        if (result && handler != null) {
            // �ɹ�������̣������ڴ��Ƴ���������������ڱ���
            return handler.process(eldest);
        }
        return result;
    }

    public void setHandler(final EldestEntryHandler<K, V> handler) {
        this.handler = handler;
    }

    /**
     * ���Ƴ����ϼ�ֵ��ʱ������øýӿڷ������д���
     */
    public interface EldestEntryHandler<K, V> {
        public boolean process(java.util.Map.Entry<K, V> eldest);
    }

}