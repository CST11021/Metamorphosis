
#Server启动流程
##1.注册ShutdownHook
###1.1将该broker从zk上注销
###1.2停止用于处理get和put请求的线程池管理器ExecutorsManager
说明：ExecutorsManager管理器包含两个线程池：

* 用于处理get请求（就是消费者从MQ拉取消息的请求）的线程池
* 用于处理无序的put请求（就是生产者向MQ发送消息的请求）的线程池

###1.3停止消息存储管理器MessageStoreManager
###1.4停止统计管理器StatsManager
###1.5关闭通讯层Server
###1.6从当前JVM中移除ShutdownHook
###1.7停止MQ服务端命令处理器CommandProcessor
###1.8其他相关服务









##2.初始化通讯层相关的服务
MateQ的通讯框架用的是阿里的gecko框架
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









##4.创建IdWorker（用于生产唯一的消息ID，全局唯一，时间有序）

当MQ服务端接收到PutCommon命令时，会使用IdWorder为该消息生成一个唯一的消息ID







##5.初始化MessageStoreManager消息存储管理器

* 创建MetaConfig#topics监听，当topics参数改变时触发监听器：这会重新初始化topic有效性的校验规则、策略删除选择器和定时删除消息文件的任务执行器

```
// 给topics参数添加监听，当topics参数改变时触发监听器：这会重新初始化topic有效性的校验规则、策略删除选择器和定时删除消息文件的任务执行器
this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
        MessageStoreManager.this.makeTopicsPatSet();
        MessageStoreManager.this.newDeletePolicySelector();
        MessageStoreManager.this.rescheduleDeleteJobs();
    }

});
```
* 创建MetaConfig#unflushInterval监听：

```
// 给unflushInterval参数（多长时间做一次消息同步，就是将消息保存到磁盘）添加监听
this.metaConfig.addPropertyChangeListener("unflushInterval", new PropertyChangeListener() {
    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
        // 开始将消息保存到磁盘的任务
        MessageStoreManager.this.scheduleFlushTask();
    }
});
```
* 创建校验topic合法性的正则表达式
* 初始化定时任务，定时将消息保存到磁盘，并开始执行将消息保存到磁盘的定时任务









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