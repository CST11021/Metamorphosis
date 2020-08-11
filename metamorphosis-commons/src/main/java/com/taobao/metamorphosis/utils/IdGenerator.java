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
package com.taobao.metamorphosis.utils;

import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.network.RemotingUtils;


/**
 * 用于生成全局唯一字符串
 * 
 * @author boyan
 */
public class IdGenerator {

    /** 生成ID的种子，这里用做前缀 */
    private String seed;
    /** 表示ID的最大长度 */
    private int length;
    /** 自增序列 */
    private final AtomicLong sequence = new AtomicLong(1);


    /**
     * Construct an IdGenerator
     */
    public IdGenerator() {
        try {
            this.seed = RemotingUtils.getLocalHost() + "-" + System.currentTimeMillis() + "-";
            this.length = this.seed.length() + ("" + Long.MAX_VALUE).length();
        }
        catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 生成唯一的ID，生成规则：IP + "-" + 当前时间戳 + "-" + 序列，例如：
     * 192.168.14.219-1534472771137-1
     * 192.168.14.219-1534472771137-2
     * 192.168.14.219-1534472771137-3
     * 
     * @return a unique id
     */
    public synchronized String generateId() {
        final StringBuilder sb = new StringBuilder(this.length);
        sb.append(this.seed);
        sb.append(this.sequence.getAndIncrement());
        return sb.toString();
    }

    /**
     * Generate a unique ID - that is friendly for a URL or file system
     * 生成唯一的ID，生成规则：IP + "-" + 当前时间戳 + "-" + 序列，例如：
     * 192-168-14-219-1534472771137-1
     * 192-168-14-219-1534472771137-2
     * 192-168-14-219-1534472771137-3
     * 
     * @return a unique id
     */
    public String generateSanitizedId() {
        String result = this.generateId();
        result = result.replace(':', '-');
        result = result.replace('_', '-');
        result = result.replace('.', '-');
        return result;
    }

    public static void main(String[] args) {
        IdGenerator idGenerator = new IdGenerator();
        System.out.println(idGenerator.generateId() + ", " + idGenerator.generateSanitizedId());
    }

}