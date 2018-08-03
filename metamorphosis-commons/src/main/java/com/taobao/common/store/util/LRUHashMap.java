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
 * LRU算法概念：
 *
 *  LRU（least recently used)是将近期最不会访问的数据给淘汰掉，其实LRU是认为最近被使用过的数据，那么将来被访问的概率也多，最近没有被访问，
 * 那么将来被访问的概率也比较低“，其实这个并不是正确的，但是因为LRU算法简单，存储空间没有被浪费，所以还是用的比较广泛的。
 *
 * @author dennis
 * 
 * @param <K>
 * @param <V>
 */
public class LRUHashMap<K, V> extends LinkedHashMap<K, V> {

    static final long serialVersionUID = 438971390573954L;

    /** HashMap最大容量 */
    private final int maxCapacity;

    /** 是否启用LRU算法 */
    private boolean enableLRU;

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** 表示移除最老键值对时的处理时限 */
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
     * 当put进新的值时，如果该方法返回true时，便移除该map中最老的键和值。
     * @param eldest 表示最老的键值对
     * @return
     */
    @Override
    protected boolean removeEldestEntry(final java.util.Map.Entry<K, V> eldest) {
        if (!this.enableLRU) {
            return false;
        }
        final boolean result = this.size() > maxCapacity;
        if (result && handler != null) {
            // 成功存入磁盘，即从内存移除，否则继续保留在保存
            return handler.process(eldest);
        }
        return result;
    }

    public void setHandler(final EldestEntryHandler<K, V> handler) {
        this.handler = handler;
    }

    /**
     * 当移除最老键值对时，会调用该接口方法进行处理
     */
    public interface EldestEntryHandler<K, V> {
        public boolean process(java.util.Map.Entry<K, V> eldest);
    }

}