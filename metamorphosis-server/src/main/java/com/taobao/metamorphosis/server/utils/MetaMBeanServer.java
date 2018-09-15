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
package com.taobao.metamorphosis.server.utils;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;


/**
 * 注册到平台Mbean server,便于在运行时修改MQ配置
 * 
 * @author boyan
 * @Date 2011-7-14
 * 
 */
public class MetaMBeanServer {

    /**
     * 将对象o注册到java的MXBean平台
     * @param o         要注册到MXBean平台的Bean对象
     * @param name      注册对象的标识名
     */
    public static void registMBean(Object o, String name) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (null != mbs) {
            try {
                mbs.registerMBean(o, new ObjectName(o.getClass().getPackage().getName() + ":type="
                        + o.getClass().getSimpleName() + (null == name ? ",id=" + o.hashCode() : ",name=" + name
                        + "-" + o.hashCode())));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}