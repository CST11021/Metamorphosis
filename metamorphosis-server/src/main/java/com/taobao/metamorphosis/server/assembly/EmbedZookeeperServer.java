package com.taobao.metamorphosis.server.assembly;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


/**
 * An embed zookeeper server.
 * 表示一个内嵌在MQ服务端的zk Server，该类主要用于获取与zookeeper通讯的相关配置，比如最大连接数、日志路径、心跳检测间隔时间等
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public final class EmbedZookeeperServer {

    static final Log logger = LogFactory.getLog(EmbedZookeeperServer.class);

    private ServerCnxnFactory standaloneServerFactory;

    private static EmbedZookeeperServer instance = new EmbedZookeeperServer();
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /** zk服务的端口号 */
    private final int port = Integer.parseInt(System.getProperty("zk.server.port", "2181"));
    /** 获取zk的心跳检测时间间隔 */
    private final int tickTime = Integer.parseInt(System.getProperty("zk.server.tickTime", "1000"));
    /** 事务的快照日志文件 */
    private final String snapDir = System.getProperty("zk.server.snapDirectory", TMP_DIR + "/" + "zookeeper");
    /** 日志目录 */
    private final String logDir = System.getProperty("zk.server.logDirectory", TMP_DIR + "/" + "zookeeper");
    /** 最大连接数 */
    private final int maxConnections = Integer.parseInt(System.getProperty("zk.server.max.connections", "4096"));

    private EmbedZookeeperServer() {

    }

    public static EmbedZookeeperServer getInstance() {
        return instance;
    }

    public void start() throws IOException, InterruptedException {
        File snapFile = new File(this.snapDir).getAbsoluteFile();
        if (!snapFile.exists()) {
            snapFile.mkdirs();
        }
        File logFile = new File(this.logDir).getAbsoluteFile();
        if (!logFile.exists()) {
            logFile.mkdirs();
        }

        ZooKeeperServer server = new ZooKeeperServer(snapFile, logFile, this.tickTime);
        this.standaloneServerFactory =
                NIOServerCnxnFactory.createFactory(new InetSocketAddress(this.port), this.maxConnections);
        this.standaloneServerFactory.startup(server);
        logger.info("Startup zookeeper server at port :" + this.port);
    }

    public void stop() {
        if (this.standaloneServerFactory != null) {
            this.standaloneServerFactory.shutdown();
        }
    }

}
