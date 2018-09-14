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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 与zk交互的工具类
 * 
 * @author boyan
 * @Date 2011-4-25
 * @author wuhua
 * @Date 2011-6-26
 */
public class ZkUtils {

    private static Log logger = LogFactory.getLog(ZkUtils.class);

    /**
     * make sure a persiste.nt path exists in ZK. Create the path if not exist.
     * 创建一个持久的节点到zk
     * @param client    zk客户端
     * @param path      节点路径
     * @throws Exception
     */
    public static void makeSurePersistentPathExists(final ZkClient client, final String path) throws Exception {
        if (!client.exists(path)) {
            try {
                client.createPersistent(path, true);
            }
            catch (final ZkNodeExistsException e) {
            }
            catch (final Exception e) {
                throw e;
            }

        }
    }

    /**
     * create the parent path
     * 创建一个目录节点
     */
    public static void createParentPath(final ZkClient client, final String path) throws Exception {
        final String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }

    /**
     * 使用给定的路径和数据创建临时节点。在必要时创建的父节点。
     * @param client    zk客户端
     * @param path      节点路径
     * @param data      节点数据
     * @throws Exception
     */
    public static void createEphemeralPath(final ZkClient client, final String path, final String data) throws Exception {
        try {
            client.createEphemeral(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    /**
     * Create an ephemeral node with the given path and data. Throw NodeExistException if node already exists.
     * 创建一个数据节点，如果已经存在则抛出异常
     *
     * @param client        zk客户端
     * @param path          节点路径
     * @param data          节点数据
     * @throws Exception
     */
    public static void createEphemeralPathExpectConflict(final ZkClient client, final String path, final String data) throws Exception {
        try {
            createEphemeralPath(client, path, data);
        }
        catch (final ZkNodeExistsException e) {

            // this canZkConfig happen when there is connection loss; make sure
            // the data
            // is what we intend to write
            String storedData = null;
            try {
                storedData = readData(client, path);
            }
            catch (final ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller
                // handles this
            }
            catch (final Exception e2) {
                throw e2;
            }
            if (storedData == null || !storedData.equals(data)) {
                throw e;
            }
            else {
                // otherwise, the creation succeeded, return normally
                logger.info(path + " exists with value " + data + " during connection loss; this is ok");
            }
        }
        catch (final Exception e) {
            throw e;
        }

    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     * 更新持久节点的数据
     *
     * @param client        zk客户端
     * @param path          节点路径
     * @param data          节点数据
     * @throws Exception
     */
    public static void updatePersistentPath(final ZkClient client, final String path, final String data) throws Exception {
        try {
            client.writeData(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
        catch (final Exception e) {
            throw e;
        }
    }

    /**
     * 读取节点数据
     * @param client    zk客户端
     * @param path      节点路径
     * @return
     */
    public static String readData(final ZkClient client, final String path) {
        return client.readData(path);
    }

    /**
     * 读取节点数据
     * @param client    zk客户端
     * @param path      节点路径
     * @return
     */
    public static String readDataMaybeNull(final ZkClient client, final String path) {
        return client.readData(path, true);
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     * 更新临时节点的数据
     *
     * @param client    zk客户端
     * @param path      节点路径
     * @param data      节点数据
     * @throws Exception
     */
    public static void updateEphemeralPath(final ZkClient client, final String path, final String data) throws Exception {
        try {
            client.writeData(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
        catch (final Exception e) {
            throw e;
        }
    }

    /**
     * 删除节点
     * @param client
     * @param path
     * @throws Exception
     */
    public static void deletePath(final ZkClient client, final String path) throws Exception {
        try {
            client.delete(path);
        }
        catch (final ZkNoNodeException e) {
            logger.info(path + " deleted during connection loss; this is ok");
        }
        catch (final Exception e) {
            throw e;
        }
    }

    /**
     * 递归删除节点
     *
     * @param client        zk客户单
     * @param path          节点路径
     * @throws Exception
     */
    public static void deletePathRecursive(final ZkClient client, final String path) throws Exception {
        try {
            client.deleteRecursive(path);
        }
        catch (final ZkNoNodeException e) {
            logger.info(path + " deleted during connection loss; this is ok");

        }
        catch (final Exception e) {
            throw e;
        }
    }

    /**
     * 获取孩子节点
     * @param client    zk客户端
     * @param path      节点路径
     * @return
     */
    public static List<String> getChildren(final ZkClient client, final String path) {
        return client.getChildren(path);
    }

    /**
     * 获取孩子节点
     * @param client    zk客户端
     * @param path      节点路径
     * @return
     */
    public static List<String> getChildrenMaybeNull(final ZkClient client, final String path) {
        try {
            return client.getChildren(path);
        }
        catch (final ZkNoNodeException e) {
            return null;
        }
    }

    /**
     * 判断节点是否存在
     * @param client
     * @param path
     * @return
     */
    public static boolean pathExists(final ZkClient client, final String path) {
        return client.exists(path);
    }

    /**
     * 截取路径的最后一部分，例如：/root/child/test => test
     * @param path
     * @return
     */
    public static String getLastPart(final String path) {
        if (path == null) {
            return null;
        }
        final int index = path.lastIndexOf('/');
        if (index >= 0) {
            return path.substring(index + 1);
        }
        else {
            return null;
        }
    }

    /**
     * String类型的数据节点转换器
     */
    public static class StringSerializer implements ZkSerializer {

        @Override
        public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
            try {
                return new String(bytes, "utf-8");
            }
            catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }

        @Override
        public byte[] serialize(final Object data) throws ZkMarshallingError {
            try {
                return ((String) data).getBytes("utf-8");
            }
            catch (final UnsupportedEncodingException e) {
                throw new ZkMarshallingError(e);
            }
        }

    }

    /** zk配置 */
    public static class ZKConfig extends Config implements Serializable {
        static final long serialVersionUID = -1L;

        /** 表示根目录，默认为"/meta" */
        @Key(name = "zk.zkRoot")
        public String zkRoot = "/meta";
        /** 是否启用zookeeper（是否将broker相关配置注册到zk） */
        @Key(name = "zk.zkEnable")
        public boolean zkEnable = true;
        /** 表示连接zk的信息，例如：127.0.0.1:2181 */
        @Key(name = "zk.zkConnect")
        public String zkConnect;
        /** zk会话超时时间 */
        @Key(name = "zk.zkSessionTimeoutMs")
        public int zkSessionTimeoutMs = 30000;
        /** the max time that the client waits to establish a connection to zookeeper */
        @Key(name = "zk.zkConnectionTimeoutMs")
        public int zkConnectionTimeoutMs = 30000;
        /** 表示zk主从节点的数据同步时间，比如一个zk机器节点发生变化，则需要同步到其他节点，这里参数表示默认需要多长时间才能同步到其他节点 */
        @Key(name = "zk.zkSyncTimeMs")
        public int zkSyncTimeMs = 5000;

        public ZKConfig() {
            super();
            this.zkConnect = "localhost:2181";
        }
        public ZKConfig(final String zkConnect, final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs, final int zkSyncTimeMs) {
            super();
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
        }
        public ZKConfig(final String zkRoot, final String zkConnect, final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs, final int zkSyncTimeMs, final boolean zkEnable) {
            super();
            this.zkRoot = zkRoot;
            this.zkConnect = zkConnect;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
            this.zkSyncTimeMs = zkSyncTimeMs;
            this.zkEnable = zkEnable;
        }

        public String getZkRoot() {
            return this.zkRoot;
        }
        public void setZkRoot(String zkRoot) {
            this.zkRoot = zkRoot;
        }
        public boolean isZkEnable() {
            return this.zkEnable;
        }
        public void setZkEnable(boolean zkEnable) {
            this.zkEnable = zkEnable;
        }
        public String getZkConnect() {
            return this.zkConnect;
        }
        public void setZkConnect(String zkConnect) {
            this.zkConnect = zkConnect;
        }
        public int getZkSessionTimeoutMs() {
            return this.zkSessionTimeoutMs;
        }
        public void setZkSessionTimeoutMs(int zkSessionTimeoutMs) {
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        }
        public int getZkConnectionTimeoutMs() {
            return this.zkConnectionTimeoutMs;
        }
        public void setZkConnectionTimeoutMs(int zkConnectionTimeoutMs) {
            this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
        }
        public int getZkSyncTimeMs() {
            return this.zkSyncTimeMs;
        }
        public void setZkSyncTimeMs(int zkSyncTimeMs) {
            this.zkSyncTimeMs = zkSyncTimeMs;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.zkConnect == null ? 0 : this.zkConnect.hashCode());
            result = prime * result + this.zkConnectionTimeoutMs;
            result = prime * result + (this.zkEnable ? 1231 : 1237);
            result = prime * result + (this.zkRoot == null ? 0 : this.zkRoot.hashCode());
            result = prime * result + this.zkSessionTimeoutMs;
            result = prime * result + this.zkSyncTimeMs;
            return result;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (this.getClass() != obj.getClass()) {
                return false;
            }
            ZKConfig other = (ZKConfig) obj;
            if (this.zkConnect == null) {
                if (other.zkConnect != null) {
                    return false;
                }
            }
            else if (!this.zkConnect.equals(other.zkConnect)) {
                return false;
            }
            if (this.zkConnectionTimeoutMs != other.zkConnectionTimeoutMs) {
                return false;
            }
            if (this.zkEnable != other.zkEnable) {
                return false;
            }
            if (this.zkRoot == null) {
                if (other.zkRoot != null) {
                    return false;
                }
            }
            else if (!this.zkRoot.equals(other.zkRoot)) {
                return false;
            }
            if (this.zkSessionTimeoutMs != other.zkSessionTimeoutMs) {
                return false;
            }
            if (this.zkSyncTimeMs != other.zkSyncTimeMs) {
                return false;
            }
            return true;
        }


    }

}