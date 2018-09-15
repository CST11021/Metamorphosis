
#Server启动流程
##1.注册ShutdownHook
###1.1将该broker从zk上注销
###1.2停止用于处理get和put请求的线程池管理器ExecutorsManager
说明：ExecutorsManager管理器包含两个线程池：

* 用于处理get请求（就是消费者从MQ拉取消息的请求）的线程池
* 用于处理无序的put请求（就是生产者向MQ发送消息的请求）的线程池

代码：

```
@Override
    public void dispose() {
        if (this.getExecutor != null) {
            this.getExecutor.shutdown();
        }

        if (this.unOrderedPutExecutor != null) {
            this.unOrderedPutExecutor.shutdown();
        }
        try {
            // shutdown：将线程池状态置为SHUTDOWN,并不会立即停止，调用该方法后，停止接收外部submit的任务，并且内部正在跑的任务和队列里等待的任务执行完后，才真正停止
            // awaitTermination：当前线程阻塞，直到等所有已提交的任务（包括正在跑的和队列中等待的）执行完或者等超时时间到
            this.getExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            this.unOrderedPutExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            // ignore
        }
    }
```

###1.3停止消息存储管理器MessageStoreManager

关闭定时将消息保存到磁盘的线程池服务

```
    @Override
    public void dispose() {
        this.scheduledExecutorService.shutdown();
        if (this.scheduler != null) {
            try {
                this.scheduler.shutdown(true);
            }
            catch (final SchedulerException e) {
                log.error("Shutdown quartz scheduler failed", e);
            }
        }
        for (final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap : MessageStoreManager.this.stores
                .values()) {
            if (subMap != null) {
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        try {
                            // 关闭分区实例
                            msgStore.close();
                        }
                        catch (final Throwable e) {
                            log.error("Try to run close  " + msgStore.getTopic() + "," + msgStore.getPartition()
                                + " failed", e);
                        }
                    }
                }
            }
        }
        this.stores.clear();
    }
```

```
/**
     * 关闭写入
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.closed = true;
        this.interrupt();
        // 等待子线程完成写完异步队列中剩余未写的消息
        try {
            this.join(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 关闭segment，保证内容都已经提交到磁盘
        for (final Segment segment : this.segments.view()) {
            segment.fileMessageSet.close();
        }
    }
```

###1.4停止统计管理器StatsManager
###1.5关闭通讯层Server
###1.6从当前JVM中移除ShutdownHook

```
// 钩子方法执行完成后要从JVM移除
if (!this.runShutdownHook && this.shutdownHook != null) {
    try {
        Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
    }
    catch (Exception e) {
        // ignore
    }
}
```
###1.7停止MQ服务端命令处理器CommandProcessor
###1.8其他相关服务









##2.初始化通讯层相关的服务
MateQ的通讯框架用的是阿里的gecko框架
gecko依赖：

```
<dependency>
    <groupId>com.taobao.gecko</groupId>
    <artifactId>gecko</artifactId>
    <version>1.1.5-SNAPSHOT</version>
</dependency>
```
初始化通讯服务对象代码：

```
/**
 * 根据配置信息创建一个用于通讯服务的{@link RemotingServer}对象
 * @param metaConfig MQ配置对象
 * @return
 */
private static RemotingServer newRemotingServer(final MetaConfig metaConfig) {
    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.setWireFormatType(new MetamorphosisWireFormatType());
    serverConfig.setPort(metaConfig.getServerPort());
    final RemotingServer server = RemotingFactory.newRemotingServer(serverConfig);
    return server;
}
```




###2.1注册命令处理器
根据客户端发起的不通命令，MQ服务会调用不同的命令处理相应的请求客户端发起的命令及服务端对应的命令处理器如下：

| 客户端发起的命令 | 服务端的命令处理器 | 说明 
---------------|-----------------|-----
| GetCommand     		|	GetProcessor     |     
| PutCommand     		|	PutProcessor     |     
| OffsetCommand     	|	OffsetProcessor  |     
| HeartBeatRequestCommand|	VersionProcessor |     
| QuitCommand     		|	QuitProcessor    |     
| StatsCommand     		|	StatsProcessor   |     
| TransactionCommand	|	TransactionProcessor|       










##3.初始化ExecutorsManager线程池服务管理器

* 初始化用于处理get请求（就是消费者从MQ拉取消息的请求）的线程池，线程数默认为CPU*10
* 初始化用于处理无序的put请求（就是生产者向MQ发送消息的请求）的线程池，线程数默认为CPU*10


```
/** 用于处理get请求（就是消费者从MQ拉取消息的请求）的线程池 */
ThreadPoolExecutor getExecutor;

/** 用于处理无序的put请求（就是生产者向MQ发送消息的请求）的线程池 */
ThreadPoolExecutor unOrderedPutExecutor;


public ExecutorsManager(final MetaConfig metaConfig) {
    super();
    this.getExecutor =
            (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getGetProcessThreadCount(),
                new NamedThreadFactory("GetProcess"));
    this.unOrderedPutExecutor =
            (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getPutProcessThreadCount(),
                new NamedThreadFactory("PutProcess"));
}
```






##4.创建IdWorker（用于生产唯一的消息ID，全局唯一，时间有序）

当MQ服务端接收到PutCommon命令时，会使用IdWorder为该消息生成一个唯一的消息ID







##5.初始化MessageStoreManager消息存储管理器

* 创建MetaConfig#topics监听，当topics参数改变时触发监听器：这会重新初始化topic有效性的校验规则、策略删除选择器和定时删除消息文件的任务执行器
* 创建MetaConfig#unflushInterval监听：
* 创建校验topic合法性的正则表达式
* 初始化定时任务，定时将消息保存到磁盘，并开始执行将消息保存到磁盘的定时任务

```
public MessageStoreManager(final MetaConfig metaConfig, final DeletePolicy deletePolicy) {
    super();
    this.metaConfig = metaConfig;
    this.deletePolicy = deletePolicy;
    this.newDeletePolicySelector();

    // 给topics参数添加监听，当topics参数改变时触发监听器：这会重新初始化topic有效性的校验规则、策略删除选择器和定时删除消息文件的任务执行器
    this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

        @Override
        public void propertyChange(final PropertyChangeEvent evt) {
            MessageStoreManager.this.makeTopicsPatSet();
            MessageStoreManager.this.newDeletePolicySelector();
            MessageStoreManager.this.rescheduleDeleteJobs();
        }

    });

    // 给unflushInterval参数（多长时间做一次消息同步，就是将消息保存到磁盘）添加监听
    this.metaConfig.addPropertyChangeListener("unflushInterval", new PropertyChangeListener() {
        @Override
        public void propertyChange(final PropertyChangeEvent evt) {
            // 开始将消息保存到磁盘的任务
            MessageStoreManager.this.scheduleFlushTask();
        }
    });

    // 创建校验topic合法性的正则表达式
    this.makeTopicsPatSet();
    // 初始化定时线程池
    this.initScheduler();
    // 开始执行将消息保存到磁盘的定时任务
    this.scheduleFlushTask();
}
```








##6.初始化StatsManager统计管理器









##7.初始化BrokerZooKeeper（用户注册（或注销）broker和topic等信息到zk）

* 初始化zkClient
* 订阅session过期监听器，当broker与zk的session过期并发起重连时，将broker和topic发布（注册）到zk
* 初始化zk上保存broker和topic信息的节点路径

```
public MetaZookeeper(final ZkClient zkClient, final String root) {
    this.zkClient = zkClient;
    this.metaRoot = this.normalize(root);
    this.consumersPath = this.metaRoot + "/consumers";
    this.brokerIdsPath = this.metaRoot + "/brokers/ids";
    this.brokerTopicsPath = this.metaRoot + "/brokers/topics";
    this.brokerTopicsPubPath = this.metaRoot + "/brokers/topics-pub";
    this.brokerTopicsSubPath = this.metaRoot + "/brokers/topics-sub";
}
```









##8.初始化JournalTransactionStore事务存储引擎

##9.初始化ConsumerFilterManager

##10.初始化BrokerCommandProcessor服务端的命令处理器

##11.初始化TransactionalCommandProcessor命令处理器


##12.注册MQServer到Mbean server平台
将MetaMorphosisBroker对象注册到java虚拟机的Mbean server平台

```
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
```