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
 * ɾ�����Թ����࣬���ڴ�����Ϣ�ļ���ɾ�����Զ���
 */
public class DeletePolicyFactory {

    /** ���ڱ��浱ǰ����֧�ֵĲ��ԣ�Map<������, DeletePolicy> */
    private static Map<String, Class<? extends DeletePolicy>> policyMap = new HashMap<String, Class<? extends DeletePolicy>>();

    static {
        // ����һ��ʱ���ɾ������
        DeletePolicyFactory.registerDeletePolicy(DiscardDeletePolicy.NAME, DiscardDeletePolicy.class);
        // �鵵����
        DeletePolicyFactory.registerDeletePolicy(ArchiveDeletePolicy.NAME, ArchiveDeletePolicy.class);
    }

    /**
     * �򹤳�ע�����
     * @param name  ������
     * @param clazz ��Ӧ���Զ���
     */
    public static void registerDeletePolicy(String name, Class<? extends DeletePolicy> clazz) {
        policyMap.put(name, clazz);
    }

    /**
     * ���ݲ������ô���һ��ɾ�����Ե�ʵ��
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