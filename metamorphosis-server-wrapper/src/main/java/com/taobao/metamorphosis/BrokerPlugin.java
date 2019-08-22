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
 *   wuhua <wq163@163.com> 
 */
package com.taobao.metamorphosis;

import java.util.Properties;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


/**
 * @author 无花
 * @since 2011-6-9 下午01:29:19
 */

interface BrokerPlugin {

    public void start();

    public void stop();

    /**
     * 初始化插件，slave broker 作为meta的插件启动，每个插件都基于一个master broker来启动
     *
     * @param metaMorphosisBroker
     * @param props
     */
    public void init(MetaMorphosisBroker metaMorphosisBroker, Properties props);

    public String name();

}