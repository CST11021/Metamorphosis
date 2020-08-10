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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * JSON序列化和反序列化的工具类，内部使用 org.codehaus.jackson 方式实现
 */
public class JSONUtils {

    static ObjectMapper mapper = new ObjectMapper();

    /**
     * 将对象序列化为JSON
     *
     * @param o
     * @return
     * @throws Exception
     */
    public static String serializeObject(final Object o) throws Exception {
        return mapper.writeValueAsString(o);
    }

    /**
     * 将JSON饭序列化为对象
     *
     * @param s
     * @param clazz
     * @return
     * @throws Exception
     */
    public static Object deserializeObject(final String s, final Class<?> clazz) throws Exception {
        return mapper.readValue(s, clazz);
    }

    /**
     * 将JSON饭序列化为对象
     *
     * @param s
     * @param typeReference
     * @return
     * @throws Exception
     */
    public static Object deserializeObject(final String s, final TypeReference<?> typeReference) throws Exception {
        return mapper.readValue(s, typeReference);
    }
}