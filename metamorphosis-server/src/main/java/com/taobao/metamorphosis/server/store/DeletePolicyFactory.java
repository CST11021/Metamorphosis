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

import java.util.HashMap;
import java.util.Map;

import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.exception.UnknownDeletePolicyException;

/**
 * 删除策略工厂类，用于创建消息文件的删除策略对象
 */
public class DeletePolicyFactory {

    /** 用于保存当前工厂支持的策略，Map<策略名, DeletePolicy> */
    private static Map<String, Class<? extends DeletePolicy>> policyMap = new HashMap<String, Class<? extends DeletePolicy>>();

    static {
        // 超过一定时间的删除策略
        DeletePolicyFactory.registerDeletePolicy(DiscardDeletePolicy.NAME, DiscardDeletePolicy.class);
        // 归档策略
        DeletePolicyFactory.registerDeletePolicy(ArchiveDeletePolicy.NAME, ArchiveDeletePolicy.class);
    }

    /**
     * 向工厂注册策略
     * @param name  策略名
     * @param clazz 对应策略对象
     */
    public static void registerDeletePolicy(String name, Class<? extends DeletePolicy> clazz) {
        policyMap.put(name, clazz);
    }

    /**
     * 根据策略配置创建一个删除策略的实例
     * @param values
     * @return
     */
    public static DeletePolicy getDeletePolicy(String values) {
        String[] tmps = values.split(",");
        String name = tmps[0];
        Class<? extends DeletePolicy> clazz = policyMap.get(name);
        if (clazz == null) {
            throw new UnknownDeletePolicyException(name);
        }

        try {
            DeletePolicy deletePolicy = clazz.newInstance();
            String[] initValues = null;
            if (tmps.length >= 2) {
                initValues = new String[tmps.length - 1];
                System.arraycopy(tmps, 1, initValues, 0, tmps.length - 1);
            }
            deletePolicy.init(initValues);
            return deletePolicy;
        }
        catch (Exception e) {
            throw new MetamorphosisServerStartupException("New delete policy `" + name + "` failed", e);
        }

    }
}