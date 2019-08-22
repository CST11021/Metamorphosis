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
package com.taobao.metamorphosis.metaslave;

import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.SlaveConfig;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;


/**
 * 代表一个向meta master同步消息数据的slaver
 *
 * @author 无花
 * @since 2011-6-23 下午01:54:11
 */
public class MetamorphosisSlaveBroker extends AbstractBrokerPlugin {

    /** slave broker从master同步消息的实现全权委托给了SubscribeHandler */
    private SubscribeHandler subscribeHandler;

    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;

        this.putSlaveProperties(this.broker, this.props);

        if (!this.broker.getMetaConfig().isSlave()) {
            throw new SubscribeMasterMessageException("Could not start as a slave broker");
        }

        try {
            this.subscribeHandler = new SubscribeHandler(this.broker);
        } catch (final MetaClientException e) {
            throw new SubscribeMasterMessageException("Create subscribeHandler failed", e);
        }
    }

    @Override
    public String name() {
        return "metaslave";
    }

    @Override
    public void start() {
        this.subscribeHandler.start();
    }

    @Override
    public void stop() {
        this.subscribeHandler.shutdown();
    }

    private void putSlaveProperties(final MetaMorphosisBroker broker, final Properties props) {
        SlaveConfig slaveConfig = new SlaveConfig();
        slaveConfig.setSlaveId(Integer.parseInt(props.getProperty("slaveId")));
        if (StringUtils.isNotBlank(props.getProperty("slaveGroup"))) {
            slaveConfig.setSlaveGroup(props.getProperty("slaveGroup"));
        } else {
            // set default slave group
            slaveConfig.setSlaveGroup(slaveConfig.getSlaveGroup() + "_" + slaveConfig.getSlaveId());
        }
        if (StringUtils.isNotBlank(props.getProperty("slaveMaxDelayInMills"))) {
            slaveConfig.setSlaveMaxDelayInMills(Integer.parseInt(props.getProperty("slaveMaxDelayInMills")));
        }
        if (StringUtils.isNotBlank(props.getProperty("autoSyncMasterConfig"))) {
            slaveConfig.setAutoSyncMasterConfig(Boolean.valueOf(props.getProperty("autoSyncMasterConfig")));
        }
        broker.getMetaConfig().setSlaveConfig(slaveConfig);

        // 重新设置BrokerIdPath，以便注册到slave的路径
        broker.getBrokerZooKeeper().resetBrokerIdPath();
    }

}