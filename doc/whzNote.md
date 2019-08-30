

## Metamorphosis介绍

​	Metamorphosis是一个高性能、高可用、可扩展的分布式消息中间件，思路起源于LinkedIn的Kafka，但并不是Kafka的一个Copy。具有消息存储顺序写、吞吐量大和支持本地和XA事务等特性，适用于大吞吐量、顺序消息、广播和日志数据传输等场景，目前在淘宝和支付宝有着广泛的应用。

###特征
* 生产者、服务器和消费者都可分布
* 消息存储顺序写
* 性能极高,吞吐量大
* 支持消息顺序
* 支持本地和XA事务
* 客户端pull，随机读,利用sendfile系统调用，zero-copy ,批量拉数据
* 支持消费端事务
* 支持消息广播模式
* 支持异步发送消息
* 支持http协议
* 支持消息重试和recover
* 数据迁移、扩容对用户透明
* 消费状态保存在客户端
* 支持同步和异步复制两种HA
* 支持group commit
* 更多……



###概念和术语

- 消息，全称为Message，是指在生产者、服务端和消费者之间传输数据。
- 消息代理：全称为Message Broker，通俗来讲就是指该MQ的服务端或者说服务器。
- 消息生产者：全称为Message Producer，负责产生消息并发送消息到meta服务器。
- 消息消费者：全称为Message Consumer，负责消息的消费。
- 消息的主题：全称为Message Topic，由用户定义并在Broker上配置。producer发送消息到某个topic下，consumer从某个topic下消费消息。
- 主题分区：也称为partition，可以把一个topic分为多个分区。每个分区是一个有序，不可变的，顺序递增的commit log。
- 消费者分组：也称为消费者集群，全称为Consumer Group，由多个消费者组成并共同消费一个topic下的消息，每个消费者消费部分消息，同一个消息可以被多个不同的消费者集群消费，但只能被同一个消费者集群中的一个消费者消费。
- 偏移量：全称为Offset，分区中的消息都有一个递增的id，我们称之为Offset，它唯一标识了分区中的消息。



### 架构示意

![image-20190722223749189](assets/image-20190722223749189.png)

​		从上图可以看出，有4个集群。其中，Broker集群存在MASTER-SLAVE结构。多台broker组成一个集群提供一些topic服务，生产者集群可以按照一定的路由规则往集群里某台broker的某个topic发送消息，消费者集群按照一定的路由规则拉取某台broker上的消息。

###总体结构

<img src="assets/MetaQ总体结构.png" width=60%/>

###内部结构
<img src="assets/MetaQ内部结构.png" width=60%/>

###源代码结构

* Client,生产者和消费者客户端
* Client-extension，扩展的客户端。用于将消费处理失败的消息存入notify(未提供),和使用meta作为log4j appender，可以透明地使用log4j API发送消息到meta。
* Commons，客户端和服务端一些公用的东西
* Example,客户端使用的例子
* http-client，使用http协议的客户端
* server，服务端工程
* server-wrapper，扩展的服务端，用于将其他插件集成到服务端，提供扩展功能
    1.Meta gergor，用于高可用的同步复制
    2.Meta slave，用于高可用的异步复制
    3.http，提供http协议支持
* Meta spout，用于将meta消息接入到twitter storm集群做实时分析
* Tools，提供服务端管理和操作的一些工具

<img src="assets/metaq模块依赖.png"/>



## 客户端使用例子说明

metamorphosis-example里面有详细的使用例子，包括：

- 普通发送消息
- 异步发送消息
- 异步单向发送消息
- 本地事务发送消息
- XA事务发送消息
- Log4j发送，log4j appender配置在main/resources目录下
- 普通消费
- 广播消费
- 批量事务消费
- 同步拉取消费

客户端依赖：

```xml
<dependency>
    <groupId>com.taobao.metamorphosis</groupId>
    <artifactId>metamorphosis-client</artifactId>
    <version>1.4.0.taocode-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>com.taobao.metamorphosis</groupId>
    <artifactId>metamorphosis-client-extension</artifactId>
    <version>1.4.0.taocode-SNAPSHOT</version>
</dependency>
```

如果打包有错误请检查是否在自己的maven库发布过客户端了



## 配置参数说明

###1.MetaQ服务器端配置

​	MetaQ服务启动时，需要在JVM携带启动的配置参数，该配置参数用于指定MQ服务器的配置文件，例如：

* Linux：-f /Users/wanghongzhan/whz/ideaProject/Metamorphosis/metamorphosis-dashboard/dev/server.ini
* windows：-f D:\myMoveDisk\6_fromGit\Metamorphosis\metamorphosis-dashboard\dev\server.ini

配置文件内容例如：

```ini
[system]
brokerId=0
numPartitions=1
serverPort=8123
unflushThreshold=0
unflushInterval=10000
maxSegmentSize=1073741824
maxTransferSize=1048576
deletePolicy=delete,168
deleteWhen=0 0 6,18 * * ?
flushTxLogAtCommit=1
stat=true

;; Update consumers offsets to current max offsets when consumers offsets are out of range of current broker's messages.
;; It must be false in production.But recommend to be true in development or test.
updateConsumerOffsets=true

[zookeeper]
zk.zkConnect=localhost:2181
zk.zkSessionTimeoutMs=30000
zk.zkConnectionTimeoutMs=30000
zk.zkSyncTimeMs=5000

;; Topics section
[topic=test]
省略...
[topic=meta-test]
省略...
```

该server.ini对应的配置源码里的MetaConfig.java类，详细的配置项如下：

| 配置项            | 说明                                                         |
| ----------------- | ------------------------------------------------------------ |
| brokerId          | 服务器集群中唯一的id，必须为整型0-1024之间，对服务器集群的定义是使用同一个zookeeper并且在zookeeper上的root path相同，具体参见zookeeper配置 |
| dataPath          | 表示消息存储在MQ服务器上的磁盘目录，默认为用户目录下的meta目录（System.getProperty("user.home") + File.separator + "meta"）<br />消息文件具体存储的目录 = dataPath + File.separator + topic + "-" + partition，例如：/Users/wanghongzhan/meta/abc-topic-0/00000000000000000000.meta，.meta为保存消息的文件。 |
| serverPort        | 表示MQ端口，默认为：8123                                 |
| dashboardHttpPort | 控制板端口，默认为：8120                                     |
| hostName          | 表示MQ服务器所在的机器，如果没有配置，默认为本地IP地址       |
| numPartitions     | 表示MQ服务器中topic的可用分区数，默认为1，分区索引从0开始，该参数可以被topic的配置覆盖。每个topic在MQ服务器上可以有多个分区，当生产者向MQ服务器发送消息时，如果客户端没有指定分区（客户端没有指定分区时， 请求中的分区索引默认是-1），则服务器会从可用（分区是可以被关闭的）的分区中，随机选择一个分区来保存消息。 |
| unflushThreshold | 表示每隔多少条消息做一次磁盘同步，默认为1000条，也就是说在掉电情况下，最多允许丢失1000条消息。 <br/>        消息生产者发送的消息，meta服务器收到后在做必要的校验和检查之后的第一件事就是写入磁盘，写入成功之后返回应答给生产者，生产者发送消息返回SendResult，如果isSuccess返回为true,则表示消息已经确认发送到服务器并被服务器接收存储。整个发送过程是一个同步的过程。保证消息送达服务器并返回结果。因此，可以确认每条发送结果为成功的消息服务器都是写入磁盘的。写入磁盘，不意味着数据落到磁盘设备上，毕竟我们还隔着一层os，os对写有缓冲。Meta有两个特性来保证数据落到磁盘上:<br/>1、每1000条（可配置），即强制调用一次force来写入磁盘设备。<br/>2、每隔10秒（可配置），强制调用一次force来写入磁盘设备。<br/>         因此,Meta通过配置可保证在异常情况下（如磁盘掉电）10秒内最多丢失1000条消息。当然通过参数调整你甚至可以在掉电情况下不丢失任何消息。虽然消息在发送到broker之后立即写入磁盘才返回客户端告诉消息生产者消息发送成功，通过unflushThreshold和unflushInterval两个参数的控制，可以保证单机消息数据的安全性，只要机器的磁盘没有永久损坏，消息总可以在重启后恢复并正常投递给消费者们。但是，如果遇到了磁盘永久损坏或者数据文件永久损坏的情况，那么该broker上的消息数据将可能永久丢失。为了防止这种情况的发生，一个可行的方案就是将消息数据复制到多台机器，类似mysql的主从复制功能（异步复制和同步功能）。<br/>        这里可设置为1，即强制每次写入立即同步到磁盘；<br/>        如果设置为<=0的情况下，服务器会自动启用group commit技术，将多个消息合并成一次再同步来提升IO性能。经过测试，group commit情况下消息发送者的TPS没有受到太大影响，但是服务端的负载会上升很多。 <br/>判断是否启用异步写入： <br/>1、如果设置为unflushThreshold <= 0的数字，则认为启动异步写入；<br/>2、如果设置为unflushThreshold = 1，则是同步写入，即每写入一个消息都会提交到磁盘；<br /> 3、如果unflushThreshold > 0，则是依赖组提交或者是超时提交 <br/><br/>        客户端每次put消息到MQ服务器时，服务器都判断是否要将该消息立即写入磁盘，判断规则如下：如果这个topic的消息的最后写入磁盘的时间 > 配置时间（unflushInterval） 或者 这个topic还没写入磁盘的消息数量 > 配置的数量（unflushThreshold），则立即写入磁盘。 |
| unflushInterval                                  | 间隔多少毫秒定期做一次磁盘sync（即将消息保存到磁盘），该值必须 > 0，默认是10秒。也就是说在服务器掉电情况下，最多丢失10秒内发送过来的消息。MessageStoreManager#FlushRunner会定期将消息管理器中的消息flush到磁盘。 |
| maxSegmentSize                                   | 表示保存到磁盘的单个消息数据文件的最大大小，即一个.mate文件的最大大小，默认为1G。默认无需修改此选项 |
| maxTransferSize                                  | 传输给消费者的最大数据大小，默认为1M，请根据你的最大消息大小酌情设置，如果太小，每次无法传输一个完整的消息给消费者，导致消费者消费停滞。可设置成一个大数来取消限制 |
| getProcessThreadCount                            | 表示处理get请求（就是消费者从MQ服务器拉取消息的请求）的并发线程数，默认为CPUS*10 |
| putProcessThreadCount                            | 处理put请求（就是生产者向MQ服务器发送消息的请求）的并发线程数，默认为CPUS*10 |
| deletePolicy                                     | 文件删除策略:"策略名称,设定值列表"，默认值为：delete,168（默认为保存7天，超过7天即删除，这里的168是小时，10s表示10秒，10m表示10分钟，10h表示10小时，不明确指定单位默认为小时<br/> |
| lastModified                                     | 表示配置文件的最后修改时间，默认为：-1，表示从未修改，当修改后会设置为当前修改时间的时间戳 |
| path                                             | 表示server.ini配置文件的绝对路径                             |
| maxCheckpoints                                   | 事务相关配置，默认值为：3，表示最大保存的checkpoint数目，超过将淘汰最老的。<br/>checkPoint相关知识： 在数据库系统中，写日志和写数据文件是数据库中IO消耗最大的两种操作，在这两种操作中写数据文件属于分散写，写日志文件是顺序写，因此为了保证数据库的性能，通常数据库都是保证在提交(commit)完成之前要先保证日志都被写入到日志文件中，而脏数据块则保存在数据缓存(buffer cache) 中，在不定期的分批写入到数据文件中。也就是说日志写入和提交操作是同步的，而数据写入和提交操作是不同步的。这样就存在一个问题，当一个数据库崩溃的时候并不能保证缓存里面的脏数据全部写入到数据文件中，这样在实例启动的时候就要使用日志文件进行恢复操作，将数据库恢复到崩溃之前的状态，保证数据的一致性。检查点是这个过程中的重要机制，通过它来确定，恢复时哪些重做日志应该被扫描并应用于恢复。 |
| checkpointInterval                               | 表示自动checkpoint间隔，默认60 * 60 * 1000L，表示1小时，单位毫秒 |
| maxTxTimeoutTimerCapacity                        | 最大事务超时时间个数，默认3万个                              |
| flushTxLogAtCommit                               | 事务日志刷盘设置，默认为1，其他值：<br/>0表示让操作系统决定；<br/>1表示每次commit都刷盘；<br/>2表示每隔一秒刷盘一次。 |
| maxTxTimeoutInSeconds                            | 事务最大超时时间，默认为60，表示一分钟，单位为秒             |
| dataLogPath                                      | 日志存储目录，默认使用dataPath                               |
| deleteWhen                                       | 何时执行删除策略的cron表达式，默认是0 0 6,18 * * ?，也就是每天的早晚6点执行处理策略。 |
| quartzThreadCount                                |                                                              |
| configFileChecksum                               | 表示配置文件的一个校验值，只要文件内容有变更就会返回一个新的数值 |
| acceptPublish                                    | 是否接收消息，默认为true；如果为false，则不会注册发送信息到zookeeper上，客户端当然无法发送消息到该broker。本参数可以被后续的topic配置覆盖 |
| acceptSubscribe                                  | 与acceptPublish类似，默认也为true；如果为false，则不会注册消费信息到zookeeper上，消费者无法发现该broker，当然无法从该broker消费消息。本参数可以被后续的topic配置覆盖 |
| stat                                             | 全局性地控制是否开启实时统计，可被topic配置覆盖，默认为false |
| updateConsumerOffsets                            | 当消费者的offset不在Broker的数据范围内，是否强制更新消费者的offset为当前最大offset。默认为false。测试开发环境建议开启此选项，生产环境不建议 |
| loadMessageStoresInParallel                      | 是否并行加载消息存储，MQ服务器上的消息是被存储在磁盘，该属性标识是否启动时并行加载数据，开启可提升启动速度。默认不开启。开启后启动日志顺序可能紊乱 |
| topics                                           | 用于保存配置文件中配置的topic                                |
|                                                  |                                                              |
|                                                  |                                                              |



###2.MetaQ客户端配置



####2.1客户端的zk配置

对应ZKConfig.java类，配置项如下：

```java
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
```

使用默认只配置，客户端只需指定zk服务器地址即可，即只需配置zkConnect参数。

#### 2.2客户端公共配置

```java
/** MQ服务器的连接信息，如果有设置，则客户端会使用设置的url连接指定的MQ服务器，否则使用zk发现服务器 */
protected String serverUrl;

/** recover本地消息的时间间隔，这里为5分钟 */
private long recoverMessageIntervalInMills = 5 * 60 * 1000L;

/** recover的线程数量，默认是CPU个数 */
private int recoverThreadCount = Runtime.getRuntime().availableProcessors();
```



####2.3消费者配置

```java
/** MetaQ的消费者是以pull模型来从服务端拉取数据并消费，这个参数设置并行拉取的线程数，默认是CPU个数 */
    private int fetchRunnerCount = Runtime.getRuntime().availableProcessors();
    /** 当上一次没有抓取到的消息，抓取线程就会sleep，这里为设置sleep的最大时间，默认5秒，单位毫秒，测试的时候可以设置少点，不然会有消费延迟的现象 */
    private long maxDelayFetchTimeInMills = 5000;
    @Deprecated
    private long maxDelayFetchTimeWhenExceptionInMills = 10000;
    /** 同步抓取的请求超时，默认10秒，通常不需要修改此参数。 */
    private long fetchTimeoutInMills = 10000;

    /** 单个消费者的id，必须全局唯一，通常用于标识分组内的单个消费者，可不设置，系统会根据IP和时间戳自动生成 */
    private String consumerId;
    /** 表示消费端的消费分区，仅在直接连接服务器的时候(即消费指定服务上的消息)有效 */
    private String partition;
    /** 第一次消费开始位置的offset，默认都是从服务端的最早数据开始消费 */
    private long offset = 0;
    /** 表示该消费者所在分组，同一分组的消费者正常情况下不会接收重复消息，共同消费某一topic */
    private String group;

    /**
     * 保存消费者已经消费的数据的offset的间隔时间，默认5秒，单位毫秒。
     * 如果设置为更大的间隔，在故障和重启时间可能重复消费的消息更多；
     * 如果设置为更小的间隔，可能给存储造成压力
     */
    private long commitOffsetPeriodInMills = 5000L;

    /** 设置每次订阅topic是否从最新位置开始消费,如果为true，表示每次启动都从最新位置开始消费,通常在测试的时候可以设置为true。*/
    private boolean alwaysConsumeFromMaxOffset = false;

		// 重试次数配置

    /** 同一条消息在处理失败情况下最大重试消费次数，默认3次，超过就跳过这条消息并调用RejectConsumptionHandler处理 */
    private int maxFetchRetries = 3;
    /**
     * 消费者一次从MQ拉取多个消息并依次消费，当多个消息中存在一个比较大的消息体时，消费者可能尝试5次（取决于{@link #maxIncreaseFetchDataRetries}配置）
     * 都无法解析出获取的数据时，这时抓取请求会自动将抓取单个消息的最大值扩大为原来的一倍，再次尝试解析，如果再次解析失败，则将该消息跳过这条消息并调用RejectConsumptionHandler处理
     * 该配置是把消息处理失败重试跟拉取数据失败重试分开，因为有时不需要处理消费失败重试，但需要自增长拉取的数据量 */
    private int maxIncreaseFetchDataRetries = 5;

		// 负载均衡策略配置

    /** 消费端的负载均衡策略，这里使用默认的负载均衡策略，尽量使得负载在所有consumer之间平均分配，consumer之间分配的分区数差距不大于1 */
    private LoadBalanceStrategy.Type loadBalanceStrategyType = LoadBalanceStrategy.Type.DEFAULT;

```

#### 2.3生产者配置

​		从源码上看，生产者没有特殊的配置，生产者和消费者都属于MQ的客户端，他们公用客户端配置，消费者有自己的配置，但是生产没有。



## Broker、生产者和消费者处理消息过程

​		每个broker都可以配置一个或多个topic，一个topic可以有多个分区，但是在生产者看来，一个topic在所有broker上的所有分区组成一个分区列表来使用。

​		在创建producer的时候，生产者会从zookeeper上获取publish的topic对应的broker和分区列表。生产者在通过zk获取分区列表之后，会按照brokerId和partition的顺序排列组织成一个有序的分区列表，发送的时候按照从头到尾循环往复的方式选择一个分区来发送消息。

​		如果你想实现自己的负载均衡策略，可以实现相应的负载均衡策略接口。

​		消息生产者发送消息后返回处理结果，结果分为成功，失败和超时。

​		Broker在接收消息后，依次进行校验和检查，写入磁盘，向生产者返回处理结果。

​		消费者在每次消费消息时，首先把offset加1，然后根据该偏移量找到相应的消息，然后开始消费。只有在成功消费一条消息后才会接着消费下一条。如果在消费某条消息失败（如异常），则会尝试重试消费这条消息，超过最大次数后仍然无法消费，则将消息存储在消费者的本地磁盘，由后台线程继续做重试，而主线程继续往后走，消费后续的消息。



##MessageSessionFactory

作用：

1、获取指定topic下的分区信息

2、创建生产消息的生产者

3、创建消费消息的消费者

4、获取指定MQ服务器上的统计信息

实现类：

![image-20181231111712303](assets/image-20181231111712303.png)

* MetaBroadcastMessageSessionFactory：广播消息会话工厂，使用这个创建的Consumer在同一分组内的每台机器都能收到同一条消息，推荐一个应用只使用一个MessageSessionFactory。

* AsyncMessageSessionFactory：用于创建异步单向发送消息的会话工厂。
  使用场景：对于发送可靠性要求不那么高,但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性，例如，收集日志或用户行为信息等场景。
  注意：发送消息后返回的结果中不包含准确的messageId,partition,offset,这些值都是-1。

* XAMessageSessionFactory：用于创建XA消息会话的工厂。

* OrderedMessageSessionFactory：需要按照消息内容(例如某个id)散列到固定分区并要求有序的场景中使用。



##SubscribeInfoManager

​		订阅信息管理器，该组件维护了Map<group, Map<topic, SubscriberInfo>>这样的一组关系，对于每个Consumer Group维护了topic的订阅情况，SubscriberInfo类维护了以下信息：

```java
public class SubscriberInfo {
    /** 消息监听器，用于消费消息 */
    private final MessageListener messageListener;
    /** 自定义的消息过滤器 */
    private final ConsumerMessageFilter consumerMessageFilter;
    /** 订阅每次接收的最大数据大小 */
    private final int maxSize;
    
   	//省略。。。
}
```

维护了各个分组下的各个topic的消息监听器和消息过滤器。

​		当MessageConsumer被创建后，会通过subscribe()接口方法来订阅topic，接口如下：

```java
MessageConsumer subscribe(String topic, int maxSize, MessageListener messageListener, ConsumerMessageFilter consumerMessageFilter)
```

消息者的创建过程如下：

```java
/**
 * 异步消息消费者的创建示例
 */
public class AsyncConsumer {

    public static void main(final String[] args) throws Exception {
      //1、初始化客户端配置：
      MetaClientConfig config = initMetaConfig();
			//2、创建消息会话工厂：
      MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

			//3、创建消费者配置：
			final String group = "meta-example";
			ConsumerConfig consumerConfig = new ConsumerConfig(group);

			//4、创建消费者：
      MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);

			//5、订阅消息，并消费消息
			final String topic = "meta-test";
      consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

          @Override
          public void recieveMessages(final Message message) {
              System.out.println("Receive message " + new String(message.getData()));
          }

          @Override
          public Executor getExecutor() {
              // Thread pool to process messages,maybe null.
              return null;
          }

      }, null);

			//6、完成消息订阅：
      consumer.completeSubscribe();
    }

}
```







##Consumer的负载均衡策略

​		我们知道，在MetaQ中Consumer会主动向MQ发起Pull消息的请求，这里Pull请求包含topic、分区、消费者分组名、拉取的起始偏移量和本次拉取的最大数据量大小，请求信息其实已经表明了该次请求要从MQ上的抓取哪些消息，那么这里的分区是如何确定的呢？就是通过client包中的LoadBalanceStrategy接口来实现，我们先看看改接口定义：

```java
public interface LoadBalanceStrategy {

    enum Type {
        DEFAULT,
        CONSIST
    }

    /**
     * 根据consumer id查找对应的分区列表
     * 
     * @param topic         分区topic
     * @param consumerId    消费者ID，消息消费者的唯一标识
     * @param curConsumers  当前可以进行拉取消息消费的消费者
     * @param curPartitions 当前的分区列表
     * 
     * @return 返回分区列表，即当前的消费者只消费从该接口返回的分区下的消息
     */
    public List<String> getPartitions(String topic, String consumerId, final List<String> curConsumers, final List<String> curPartitions);

}
```

该接口只有一个方法，该方法表明了当Consumer要从MQ上拉取消息的时候，只能从哪些分区上拉取消息。



​		说到metaq的消费者balance策略，不得不说一下分区的有关信息。一个topic可以划分为n个分区。每个分区是一个有序的、不可变的、顺序递增的队列。

​		分区一方面是为了增大消息的容量（可以分布在多个分区上存，而不会限制在单台机器存储大小里），二方面可以类似看成一种并行度。

​		消费者的负载均衡与topic的分区数据紧密相关，需要考虑几种情况：

* 1、单个分组内的消费者数目如果比总的分区数目多的话，则多出来的消费者不参与消费。每个分区针对每个消费者group只挂一个消费者，同一个group的多余消费者不参与消费。
* 2、如果分组内的消费者数目比分区数目小，则有部分消费者要额外承担消息的消费任务。当分区数目n大于单个group的消费者数目m时，则有n%m个消费者需要额外承担1/n的消费任务。n足够大的时候可以认为负载平均分配。

综上所述，单个分组内的消费者集群的负载均衡策略如下：

*             ①每个分区针对一个group只挂载一个消费者

*             ②如果同一个group的消费者数目大于分区数目，则多出来的消费者不参与消费

* ③如果同一个group的消费者数目小于分区数目，则有部分消费者需要额外承担消费任务。

  

​		meta客户端处理消费者的负载均衡方式：将消费者列表和分区列表分别排序，然后按照上述规则做合理的挂载。如果某个消费者故障，其他消费者会感知到这一变化，然后重新进行负载均衡，保证所有分区都有消费者进行消费。
Consumer的balance策略实现在metaq中提供了两种：ConsisHashStrategy和DefaultLoadBalanceStrategy。

### DefaultLoadBalanceStrategy

​		默认的负载均衡策略，尽量使得负载在所有consumer之间平均分配，consumer之间分配的分区数差距不大于1。

###ConsisHashStrategy

​		[基于一致性哈希的负载均衡策略](https://www.jianshu.com/p/e968c081f563)



细心的读者可能会问，该接口返回的是多个分区，而Pull请求中却明确声明了一个分区，那么如何从返回的多个分区中确定一个本次请求要从哪个分区Pull消息呢？我们知道MQ客户端默认会有cpus个线程并行一直从MQ上pull消息，每个线程都会遍历所有的topic，然后再遍历所有的分区，最后将pull请求放到pull的请求队列中。当消息被拉取下来后，MQ客户端通过监听机制通知所有监听了该topic的consumer进行消费。

注意：这里的MQ客户端起始已经明确了多个consumerId，他们别人在不同的consumer group。





##答疑

###服务端FAQ



####MetaQ如何保证高可用，说说同步/异步复制

​		Metaq相比于kafka的一个重要特性就是消息高可用方案的实现，我们称之为HA方案（说明：*HA*是Hig*h A*vailable缩写，是双机集群系统简称，指高可用性集群，是保证业务连续性的有效解决*方案*，一般有两个或两个以上的节点，且分为活动节点及备用节点）。

​		消息在发送到broker之后确保写入磁盘没问题（一般先缓存到内存，然后在通过异步的方式写入磁盘、也可以配置立即写入磁盘）才返回客户端告诉消息生产者消息发送成功，通过`unflushThreshold`和`unflushInterval`两个参数的控制，可以保证单机消息数据的安全性，只要机器的磁盘没有永久损坏，消息总可以在重启后恢复并正常投递给消费者们。但是，如果遇到了磁盘永久损坏或者数据文件永久损坏的情况，那么该broker上的消息数据将可能永久丢失。为了防止这种情况的发生，一个可行的方案就是将消息数据复制到多台机器，类似mysql的主从复制功能。

​	**同步复制和异步复制**

​		Metaq提供类似mysql主从复制的异步复制和同步功能，分别对应不同的可靠级别。理论上说同步复制能带来更高的可靠级别，异步复制因为延迟的存在，可能会丢失极少量的消息数据，相应地，同步复制会带来性能的损失，因为要同步写入两台甚至更多的broker机器上才算写入成功。

​		在实际实践中，**我更推荐采用异步复制的架构**，因为异步复制的架构相对简单，并且易于维护和恢复，对性能也没有影响。而同步复制对运维要求相对很高，机制复杂容易出错，故障恢复也比较麻烦。**异步复制加上磁盘做磁盘阵列**，足以应对非常苛刻的数据可靠性要求。

​	**异步复制配置**

​		假设你已经根据[如何开始](https://github.com/killme2008/Metamorphosis/wiki/%E5%A6%82%E4%BD%95%E5%BC%80%E5%A7%8B)这份文档配置了一台broker服务器，并且配置了一个topic为`test`，现在你希望test能复制到另一台slave broker上来保证消息数据的高可用。你可以这样做：

1.首先，你需要部署一个新的broker，具体仍然参照[如何开始](https://github.com/killme2008/Metamorphosis/wiki/%E5%A6%82%E4%BD%95%E5%BC%80%E5%A7%8B)这份文档，配置server.ini从master broker拷贝一份。

2.其次，配置slave文件。编辑conf/async_slave.properties：

```properties
#slave编号,大于等于0表示作为slave启动,同一个master下的slave编号应该设不同值.
slaveId=0

#作为slave启动时向master订阅消息的group,如果没配置则默认为meta-slave-group
#不同的slaveId请使用不同的group
slaveGroup=meta-slave-group

#slave数据同步的最大延时,单位毫秒  
slaveMaxDelayInMills=500

#是否自动从master同步server.ini, 1.4.2新增选项
#第一次仍然需要自己拷贝server.ini，后续可以通过设置此选项为true来自动同步
autoSyncMasterConfig=true
```

配置参数的含义请自己看注释。可见，一个master可以复制到多个slave。

3.执行下列命令启动slave:

```bash
bin/metaServer.sh start slave
```

4.第一次复制因为需要跟master完全同步需要耗费一定时间，你可以在数据文件的目录观察复制情况。

5.请注意，异步复制的slave将参与消费者的消费活动，消息消费者可以从slave中获取消息并消费，消费者会随机从master和slaves中挑选一台作为消费broker。

6.请注意，从1.4.2开始，可以通过autoSyncMasterConfig选项配置是否自动同步master的server.ini到异步复制的slave上，当master的server.ini文件变更并通过bin/metaServer.sh reload之后，slave将监控到这一变更并自动同步。

**异步复制的局限**

- 异步复制有延迟，虽然可以通过设定`slaveMaxDelayInMills`来控制延迟。

**异步复制的故障处理**

- Master永久故障： 将slave作为master启动，去除启动参数中的slave即可，也就是`metaServer.sh restart`
- Slave永久故障： 启动新的broker并配置作为master新的slave启动。

**同步复制配置**

TODO

####消费者是否会从slave服务器拉取消息

​		会；异步复制的slave将参与消费者的消费活动，消费者可以从slave中获取消息并消费，消费者会随机从master和slaves中挑选一台作为消费broker；

​		消费者可以通过配置，从指定的MQ服务器拉取消息，否则使用zk服务发现选取一台MQ服务器拉取消息。



####消息发送成功后，已经写入服务器磁盘？

​		可以说是，也可以说不是，因为存在os和磁盘缓存。每条消息在返回应答前都先write到MQ，也就说MQ服务器可以确保接收到了来自客户端的消息，并且正确的写到了内存，但是由于缓存机制，在断电或者磁盘损坏的情况下，可能会导致内存里的消息没有写入到磁盘。

​		MQ可以确保每次读取到消息并写入到磁盘后再response给客户端，但是这样会影响性能。

​		默认情况下，MQ的机制是每1000条消息force一次和每10秒force一次（可为全局或某个Topic配置参数），也可以配置使用Group commit的方式。

####消息是怎么保存的？

​		每条消息保存在一个分区，分区内是一系列文件，顺序写，固定大小切换文件。

​		metaq的topic可以配置多个分区，每个消息put到mateq服务器后，会保存到对应topic和分区目录下，比如：~/meta/test-0（test表示topic，0表示分区索引），每个分区目录下会保存多个.meta的消息文件，注意一个消息文件保存多个消息对象，消息文件名以上一个消息文件大小命名。

​		MessageStore采用Segment方式组织存储（Segment可以看做一个.meta的消息文件），Segment包装了FileMessageSet（封装了.meta文件内的消息集合），由FileMessageSet进行读写，MessageStore将多个Segment，进行前后衔接，衔接方式为：

​		第一个Segment对应的消息文件命名为0.meta;

​		第二个则命名为第一个文件的开始位置+第一个Segment的大小;

​		图示如下(假设现在每个文件大小都为1024byte)：

​		0.meta -> 1024.meta -> 2048.meta -> ...

​		为什么要这样进行设计呢，主要是为了提高查询效率（消费者从MQ拉取消息时，每次都是指定一个offset偏移量，MQ会根据该偏移量快速定位本次要拉取的消息位于哪个文件）。MessageStore将最后一个Segment变为可变Segment，因为最后一个Segment相当于文件尾，消息是有先后顺序的，必须将消息添加到最后一个Segment上。每次消息写入都会写到最后一个消息文件中，并且一点文件写满，则该文件会被设置为只读，只有最后一个消息文件才可以写入消息。

####MQ上持久化的消息数据保留多久的，消息文件删除策略有哪些，怎么实现的？

​		MQ默认为保存7天，超过7天即删除。不过MQ支持配置，可以根据业务需求为每个Topic配置不同的保留时间。



**消息文件删除策略有两种：**

1、超过一定时间的删除策略
2、消息归档策略

```java
/**
 * 文件删除策略:"策略名称,设定值列表"，默认为保存7天
 * deletePolicy: 数据删除策略，默认超过7天即删除,这里的168是小时，10s表示10秒，10m表示10分钟，10h表示10小时，不明确指定单位默认为小时。
 * delete是指删除，超过指定时间的数据文件将被彻底从磁盘删除。
 *
 * 也可以选择archive策略，即不对过期的数据文件做删除而是归档，当使用archive策略的时候可以选择是否压缩数据文件，如167,archive,true
 * 即选择将更改时间超过7天的数据文件归档并压缩为zip文件，如果不选择压缩，则重命名为扩展名为arc的文件。
 */
private String deletePolicy = "delete,168";
```

​		消息文件删除策略是可配置的，MQ服务启动时，会从配置文件中读取配置的删除策略，如果没有配置，默认选择保存7天的删除策略。

​		并且MQ可以为每个topic配置不同的文件删除策略。

####消息存储管理器多长时间做一次消息同步(将消息保存到磁盘)？

​		MQ通过配置参数来控制，消息同步，并且消息存储器初始化时，会监听配置对象的unflushInterval参数，当该参数改变时会重新初始化将消息保存到磁盘的定时任务。

```java
/**
  * 间隔多少毫秒定期做一次磁盘sync（将消息保存到磁盘）,默认是10秒。也就是说在服务器掉电情况下，最多丢失10秒内发送过来的消息。不可设置为小于或者等于0。
  * MessageStoreManager#FlushRunner会定期将消息管理器中的消息flush到磁盘
  */
private int unflushInterval = 10000;
```

####消息发送到服务器时，MetaQ是如何处理的？

​		首先，MQ服务器启动后会一直监听生产者发送过来的消息并进行处理。

​		MQ监听到PutCommand请求后，metaq会先记录到统计管理器中，然后判断该请求携带的topic和对应的分区是否关闭，如果分区被关闭，会返回Forbidden（状态码403）给客户端；

​		如果没有关闭，会从请求中获取对应topic和partition，如果生产者没有指定消息要保存的partition，则MQ会使用随机的方式从对应的topic的有效partition中随机获取一个分区，然后MQ使用IdWorker（twitter项目snowflake的id产生方案，全局唯一，时间有序）生产一个全局唯一的消息ID；

​		然后再根据topic和partition获取（或创建）MessageStore，然后将消息append到MessageStore中。当MQ确保将消息write到MessageStore后，再response给客户端，告诉客户端消息已经保存到了MQ中。

####消息存储管理器是怎么设计的？

​		MessageStoreManager维护了topic，partition和对应消息存储器

```java
/** Map<topic, Map<partition, MessageStore>> 用于存储消息 */
ConcurrentHashMap<String, ConcurrentHashMap<Integer, MessageStore>> stores；
```

​		消息append到MessageStore后，MQ会判断是否启动异步写入消息，如果是使用异步的方式，则将消息保存到队列中，然后通过异步的方式flush到磁盘；如果是同步的方式也会先将消息保存到内存，然后会立即判断一次MessageStore中消息的数量是否满足写入磁盘条件，如果满足则立即写入，如果不满足则先将消息保存在内存，等待MessageStoreManager中的定时任务执行时或下次该对应的topic有消息put到MQ时，则再次判断消息是否满足写入磁盘条件。

​	MessageStoreManager会为每个topic维护一个定时删除消息文件的任务，当MessageStoreManager被初始化后，会开始定时执行这些任务。

```java
* 判断是否启用异步写入：
* 1、如果设置为unflushThreshold <= 0的数字，则认为启动异步写入；
* 2、如果设置为unflushThreshold = 1，则是同步写入，即每写入一个消息都会提交到磁盘；
* 3、如果unflushThreshold > 0，则是依赖组提交或者是超时提交
```

####当topic变更时，MQ的消息存储器会做些什么？

​		消息存储器初始化时，会保存一份该服务的topics，然后监听配置对象中topics参数，当topics参数改变时会触发监听器：这时会重新初始化topic有效性的校验规则、文件删除策略选择器和定时删除消息文件的任务执行器。

####MQ单次消息传递最大数据大小是多少

​		MQ的消息体大小是可配置的，通过maxTransferSize参数配置，默认为1M，请根据你的最大消息大小酌情设置。以mateq下发消息为例，如果太小，每次无法传输一个完整的消息给消费者，导致消费者消费停滞，可设置成一个大数来取消限制。



####MQ的消息类型可以是文件吗？

​		理论上是可以的，因为消息传输也是通过字节的方式存储的，但是不建议直接在消息里存储文件，因为文件数据一般比较大，MQ的消息数据大小是有限制的

####metaq消息过滤器的使用

​		消息过滤器可以在消费端使用也可以在服务端使用，服务端使用的实现机制是，当消费者从MQ拉取消息时，MQ会根据配置的过滤器来过滤消息，服务端过滤消息需要自己实现消息过滤器，并打成jar包到meta安装目录下，然后在topic配置中配置对应的过滤器，例如如下配置，这里是直接在meta源码中添加了一个过滤器

```ini
[system]
appClassPath=/Users/wanghongzhan/.m2/repository/com/taobao/metamorphosis/metamorphosis-server
省略...

[topic=meta-test]
group.meta-example=com.taobao.metamorphosis.server.filter.ExampleConsumerMessageFilter
省略...
```























###消费者FAQ

#### 客户端消费失败，MQ如何处理的？

​		客户端消费失败分为消息拉取失败和消费者处理消息失败，不管什么失败都会将消息请求重新放回请求队列，等待下次重新从MQ服务器拉取消息，重新处理，如果客户端处理失败次数大于 > 3次（可配置），则会调用拒绝处理消息的策略接口，处理消费失败的消息，该策略接口有两种处理方式，一个是直接放弃该消息，一个是将消息保存在本地，等待下次消费者重启服务时，重新消费该消息。

#### 客户端会重复消费消息吗？



#### #消费者是如何从服务端拉取消息进行消费的？

​		消息订阅时只是保存topic与MessageListener和ConsumerMessageFilter的映射关系，并将topic保存到订阅注册表（该注册表保存在客户端机器的内存中），此时还没启动异步线程从MQ服务器拉取消息。

* MessageListener即消息监听器，用于处理从服务器拉取的消息；

* ConsumerMessageFilter即消息过滤器，用于消费者过滤消息，在某些场景下你可能只想消费一个topic下满足一定要求的消息将消息监听器和消息过滤器添加到topic订阅信息管理器中。

  当执行completeSubscribe时，消费者会遍历所有订阅的topic，然后根据每个topic创建一个抓取请求对象，并放到消息抓取管理器的请求队列中，然后再启动消息抓取管理器开启多个线程（默认是cpu个数）从服务器抓取消息，抓取到消息后会通知对应的消息监听器来处理消息。

#### #消费者抓取消息的请求都包含哪些信息？

​		包括topic、拉取的目的分区、消费者分组名称、拉取的起始偏移量和本次拉取的最大数据量大小。

​		MQ服务端将多个消息保存为一个消息文件，并存放在对应的分区目录下，每次消费者从服务器端拉取消息时，都会指定拉取的信息，然后服务端根据偏移量和最大数据量大小，从消息文件切入分配信息，也就是截取一部分消息返回给消费者，消费者再依次处理拉取的这批消息。MQ将消息拆分为多个消息文件是为了便于当接收到客户端拉取请求的时候，能快速查找到对应的消息的所在文件，比如，假设单个消息大小都是1byte，现在有三个消息文件0.mate、1024.mate和2048.mate，当抓取请求的偏移量是1000，最大数据量大小是50时，则MQ就能快速定位到该批次的消息存放在第二文件内，并将该文件读到内存，从进行分片，找到1000到1024这个区间对应的消息，因为偏移量从1025开始就不存放在第二个文件，所以，只返回24个消息，后面的25个消息只能等下次抓取请求到来的时候再下发给消费者。将文件拆分为多个文件还有一个好处是，每次将文件读到内存时，不用占用太大的内存空间。

#### #消费者拉取消息时都会有一个偏移量，这个偏移量是什么

​		这里偏移量可以理解大约理解为消息体的大小，但是实际上比消息要大一点，每次从mq指定topic分区拉取消息时，都会指定拉取的起始索引开始，每个偏移量的步长大约等于消息体的大小，下一次拉取的时候指向下一个消息。

​		一般消息文件的偏移量每次改变时都是偏移好几个消息体，因为消费者从服务端拉取消息，虽然每次只消费一个消息，但是从服务端拉取消息时，消费者会一次拉取多个消息，并依次处理消息。



Offset存储

​                MetaQ的消费模型是一种拉取的模型，消费者根据上次消费数据的绝对偏移量(offset)从服务端的数据文件中拉取后面的数据继续消费，因此这个offset信息就非常关键，需要可靠地保存。默认情况下，MetaQ是将offset信息保存在你使用的zookeeper集群上，也就是`ZkOffsetStorage`所做的事情，它实现了`OffsetStorage`接口。通常这样的保存是可靠并且安全的，但是有时候可能你也需要其他选项，目前还提供两个不同的`OffsetStorage`实现：

- `LocalOffsetStorage`，使用consumer的本地文件作为offset存储，默认存储在`${HOME}/.meta_offsets`的文件里。适合消费者分组只有一个消费者的情况，无需共享offset信息。例如广播类型的消费者就特别合适。
- `MysqlOffsetStorage`，使用Mysql作为offset存储，使用前需要创建表结构：

```sql
CREATE TABLE `meta_topic_partition_group_offset` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topic` varchar(255) NOT NULL,
  `partition` varchar(255) NOT NULL,
  `group_id` varchar(255) NOT NULL,
  `offset` int(11) NOT NULL,
  `msg_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `TOPIC_PART_GRP_IDX` (`topic`,`partition`,`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

你也可以实现自己的`OffsetStorage`存储。如果你想使用除了zookeeper之外的offset存储，可以在创建消费者的时候传入：

```java
  consumer sessionFactorycreateConsumer(consumerConfig, (dataSource));
```

mysql存储需要传入JDBC数据源。

​        第一次消费的offset初始值。

​        前面提到`ConsumerConfig`有个`offset`参数可以设置第一次消费的时候开始的绝对偏移量，默认这个参数是0，也就是从服务端现有消息的最小偏移量开始，从头开始消费所有消息。

​		但是，通常情况下，新的消费分组都是希望从最新的消息开始消费，`ComsumerConfig`提供了一个`setConsumeFromMaxOffset(boolean always)`方法来设置从最新位置开始消费。其中`always`参数表示是否每次消费者启动都从最新位置开始消费，这样就忽略了在消费者停止期间的消息。通常仅在测试的时候将`always`参数设置为true，以便每次测试最新的消息。除非你真的不需要消费者停止期间（比如重启间隔）的消息，否则不要将always设置为真。



#### #消费者分组的作用？

​		在MetaQ里，消费者被认为是一个集群，也就是说认为是有一组的机器在共同分担消费一个topic。因此消费者配置`ConsumerConfig`中最重要的配置是group，每个消费者都必须告诉MetaQ它属于哪个group，然后MetaQ会找出这个group下所有注册上来的消费者，在他们之间做负载均衡，共同消费一个或多个topic。注意，不同group之间可以认为是不同的消费者，他们消费同一个topic下的消息的进度是不同。

​	举例来说，假设你有一个topic为`business-logs`，是所有业务系统的日志。然后现在你对这些日志要做两个事情：一个是存储到HDFS这样的分布式文件系统，以便后续做分析处理；以个是Twitter Storm这样的实时分析系统，做实时的数据分析、告警和展现。显然，这里你就需要两个group，比如我们有一个group叫`hdfs-writer`，它有三台机器同时消费`business-logs`，将日志存储到HDFS集群。同时，你也有另一个group叫`storm-spouts`，有5台机器用来给storm集群喂数据。这两个group是隔离，虽然是消费同一个topic，但是两者是消费进度（消费了多少个消息，等待消费多少个消息等信息）是不同的。但是同一个group内，例如`hdfs-writer`的三台机器，这三台机器是共同消费`business-logs`下的消息，同一条消息只会被这`hdfs-writer`三台机器中的一台处理，但是这条消息还会被`twitter-spouts`等其他分组内的某一台机器消费。

#### #为什么在调用subscribe还要调用一次completeSubscribe?

​		因为subscribe可以调用多次，为了减少跟zk交互次数，subscribe会将订阅信息保存在内存，completeSubscribe的时候一次性处理。

​		一个消费者实例可以订阅多个topic，每个topic都有对应的MessageListener消息监听器（消息处理器）和ConsumerMessageFilter（消息过滤器），每个topic最多对应一个MessageListener和一个ConsumerMessageFilter。

#### #消息可以带属性吗？

​		仅允许带一个字符串属性，消费者可依此过滤

#### #消息ID怎么产生？

–Long类型，在发送成功后由服务器端返回

–默认-1

–42位时间 + 10 位 brokerId + 12位递增数字

#### #消息体怎么产生？

–消息体只要求是一个byte[]数组

–序列化方式完全由用户决定



#### #消息是按顺序消费吗



#### #实时性问题如何解决?

–  服务端提高刷盘频率，客户端减少pull时间间隔

–    [ConsumerConfig](eclipse-javadoc:%E2%98%82=metamorphosis-client/src\/main\/java%3ccom.taobao.metamorphosis.client.consumer%7bConsumerConfig.java%E2%98%83ConsumerConfig).setMaxDelayFetchTimeInMills(long maxDelayFetchTimeInMills)



#### 消费者是单线程还是多线程拉消息?



–  多线程（默认为CPU的个数），也可以配置只有一个线程拉

–  [ConsumerConfig](eclipse-javadoc:%E2%98%82=metamorphosis-client/src\/main\/java%3ccom.taobao.metamorphosis.client.consumer%7bConsumerConfig.java%E2%98%83ConsumerConfig).setFetchRunnerCount(int fetchRunnerCount)



####处理消息的回调方法是运行在单线程还是多线程中?

–多线程拉，不同分区消息的回调是运行在多线程环境中的

–多线程拉，相同分区消息的回调可以认为是运行在单线程环境中的

单线程拉，运行在单线程中



####Pull的偏移量保存在哪里？

–默认保存在zk

–我们还提供文件、数据库的存储实现。

–OffsetStorage接口，可自主实现。



####偏移量多长时间保存一次？

–默认5秒，可设置

–**ConsumerConfig.setCommitOffsetPeriodInMills(long commitOffsetPeriodInMills)**



####新加入的消费者不想接收到以前发的消息怎么办？

–新增的group和广播新增的机器有这个问题

–1.3-SNAPSHOT及其之后的版本支持可设置

ConsumerConfig.setConsumeFromMaxOffset



####消息处理失败如何重试？

–可选择跳过，设置最大重试次数，超过即跳过，默认5次

–**ConsumerConfig.setMaxFetchRetries(int maxFetchRetries)**

–跳过的消息将保存在消费者本地磁盘或者notify，并自动Recover重试。

–如果不想往下走就把这个参数设为int的最大值

####消息能保证不重复接收吗？

–因为每个分区物理隔离消息，理论上每个消费者接收的消息不会重复

–在consumer重新负载均衡的时候，可能由于offset保存延迟，导致重复接收极小部分消息。



####可以设置pull请求的时间间隔吗？

–可以，你可以设置允许的最大延迟时间，当响应为空的时候，每次递增最大延迟时间的1/10做延迟，不会超过设定的最大延迟时间。默认5秒。



####一个consumer可以有多个consumer group吗

不可以，创建consumer时，必须指定一个consumer group

```java
final String group = "meta-example";
ConsumerConfig consumerConfig = new ConsumerConfig(group);
// 默认最大获取延迟为5秒，这里设置成100毫秒，请根据实际应用要求做设置，测试的时候如果使用默认值5秒，会有消费延迟的现象
consumerConfig.setMaxDelayFetchTimeInMills(100);
final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
```

###生产者FAQ

#### 生产者发送消息时，如果发送失败会进行重试吗？

​		默认不会重试，生产者发送消息分为同步和异步两种，这两种都有返回消息发送的结果，开发人员可以通过该结果对象进行处理消息发送失败时的处理机制。

​		当broker server增加或减少时，client会重新进行负载均衡。Broker减少的瞬间，在负载均衡之前，已经发送到减少的那台broker但未到达服务器时，客户端将会捕获到发送异常，由业务决定如何处理。如果业务决定重试，当负载均衡之后将正常发送到其他服务器上。



#### 生产者发送消息是有序的吗？

- 只保证单线程发送的消息有序；
- 只保证发送同一个分区的消息有序；
- 实现自定义分区选择器和故障情况下的本地存储功能



#### 什么场景下适合使用异步单向和log4j发送

1. 对于发送可靠性要求不那么高，但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性.
2. 不在乎发送结果成功与否。
3. 从逻辑和耗时上几乎不对业务系统产生影响



#### 生产者发布topic时，做了哪些事情？

1. 首先，在zk上创建/meta/brokers/topics-pub/${topic}路径，并监听该路径子节点的变更情况；
2. 然后获取topic对应的MQ服务器信息，生产者断开与所有旧的MQ服务器的连接，然后再重新连接到新的MQ集群；
3. 如果topic对应的MQ集群发生变更，则执行所有监听了该topic的监听器。



#### 为什么发送消息前需要先发布topic，可以不发布吗?

​	发布topic是为了根据topic从zk获取有效的broker列表，生产者发布消息前，必须先发布topic；

​	生产者发布topic时，如果之前该topic已经注册到zk上了，则无需在服务端配置topic和重启MQ服务器（或重新加载配置文件），否则，生产者在发布topic之前，需要在MQ的conf目录下topic.ini（老版本在server.ini）文件中添加topic的配置，并重启server（bin目录start.sh）或者重载（tool包bin目录下reloadconfig.sh）。

#### 消息的生产者有哪几种实现，有什么区别？

​	共有4种不同的实现，分别是：

* SimpleMessageProducer：默认的生产者，其他3种都继承该类，他提供了同步发送消息和异步发送消息的方法，异步的方式通过gecko通信框架的回调回调机制来实现的，这两种方式的消息发送失败都不会进行重试。此外，该实现还提供本地事务的消息生产者实现。

* AsyncMetaMessageProducer：异步单向发送消息给服务器的生产者实现，最大限度的减少对业务主流程的影响，使用者可以不关心发送成功或失败和所有异常，发送消息后返回的结果中不包含准确的messageId,partition,offset,这些值都是-1。
* SimpleXAMessageProducer：基于Atomikos分布式事务实现的消息生产者实现。

* OrderedMessageProducer：有序消息生产者的实现类，Metamorphosis对消息顺序性的保证是有限制的，默认情况下，消息的顺序以谁先达到服务器并写入磁盘，则谁就在先的原则处理。并且，发往同一个分区的消息保证按照写入磁盘的顺序让消费者消费，这是因为消费者针对每个分区都是按照从前到后递增offset的顺序拉取消息。

  Meta可以保证，在单线程内使用该producer发送的消息按照发送的顺序达到服务器并存储，并按照相同顺序被消费者消费，前提是这些消息发往同一台服务器的同一个分区。为了实现这一点，你还需要实现自己的PartitionSelector用于固定选择分区

  ```java
  public interface PartitionSelector {
      public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
  }
  ```

  选择分区可以按照一定的业务逻辑来选择，如根据业务id来取模。或者如果是传输文件，可以固定选择第n个分区使用。当然，如果传输文件，通常我们会建议你只配置一个分区，那也就无需选择了。

  消息的顺序发送在1.2这个版本提供了OrderedMessageProducer，自定义管理分区信息，并提供故障情况下的本地存储功能。



#### 分区选择都有哪些实现？

​		分区选择器：生产者发送消息时，会使用分区选择器选择一个分区保存消息，默认使用循环的方式选择分区。生产者在通过zk获取分区列表之后，会按照brokerId和分区号的顺序排列组织成一个有序的分区列表，发送的时候按照从头到尾循环往复的方式选择一个分区来发送消息。这是默认的分区策略，MetaQ还提供随机分区选择器，此外，考虑到我们的broker服务器软硬件配置基本一致，默认的轮询策略已然足够。如果你想实现自己的负载均衡策略，可以自己实现PartitionSelector接口，并在创建producer的时候传入即可。



#### 生产者发送消息时，如何确定消息发送到那个MQ服务器？

​		生产者只会将消息保存到master服务器，然后slave从master同步消息，当Master永久故障时，可以将slave作为master启动。

​		另外，消费者也会从master或slaver拉取消息进行消费，因为消息是持久化在MQ服务器上的（默认保留七天），消费者消费完消息后会将偏移量保存到zk，下次再次拉取时会根据偏移量重新获取消息，所以无论消息保存在master或slaver，对于消费者来说一样的。但是生产者只会将消息保存到master，因为slaver会从master同步消息，但是master不会从slaver同步消息。

#### 生产者会push消息到slaver服务器吗

​		不会。

#### 消息的生产者支持的事务消息机制是什么？





###经典案例分析

#### 关于消费者Id的一个坑

如下消费者的创建到消费消息的代码：

```java
public class AsyncConsumer {

    public static void main(final String[] args) throws Exception {
        // 1、初始化客户端配置
        MetaClientConfig config = initMetaConfig();

        // 2、创建消息会话工厂：一般会话工厂会使用单例来创建
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(config);

        // 3、创建消费者
        final String group = "meta-example";
        ConsumerConfig consumerConfig = new ConsumerConfig("consumer1", group);
        // 默认最大获取延迟为5秒，这里设置成100毫秒，请根据实际应用要求做设置，测试的时候如果使用默认值5秒，会有消费延迟的现象
        consumerConfig.setMaxDelayFetchTimeInMills(100);
        final MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);

        // 4、订阅消息，并消费消息
        final String topic = "meta-test";
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            @Override
            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
            }

            @Override
            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }

        });
        consumer.completeSubscribe();
    }

}
```

这里ConsumerConfig#consumerId设置为"consumer1"，假如meta-test这个topic对应的分区个数配置为5时，则，zk上注册的负载均衡结果为：

<img src="/Users/wanghongzhan/1_Document/ideaProject/Metamorphosis/doc/assets/image-20190728175706061.png"/>

说明：0-0、0-1、0-2、0-3、0-4表示0这个brokerId对应的四个分区，对应表示消费者，此时0到4这四个分区都可以被consumer1这个消费者消费，那么假如消费者所在的应用是集群部署的话，这里的消息就会被每个节点消费，导致每个应用都会消费一次MQ上的消息，也就说假如消费者应用集群为n时，则单个消息会被重复消息n次。如果要设置consumerId则每个应用在发布的时候需要确保配置为不同consumerId才行。

所以在创建消费者时，ConsumerConfig#consumerId不建议设置，当consumerId为空时，MQ会自动帮我们创建不同的consumerId，消费者的负载均衡结果如下：

<img src="/Users/wanghongzhan/1_Document/ideaProject/Metamorphosis/doc/assets/image-20190728180719463.png"/>

说明：0-1、0-1、0-2这个三个分区被第一个节点消费，然后0-3、0-4会被第二节点消费，此时消息就不会被重复消费，一个消息只会被一个消费者应用节点消费一次。

有同学可能会觉得奇怪一个应用节点依然对应多个分区，为什么消息就不会被重复消费了呢？其实，在consumerId设置为"consumer1"这种情况时，虽然0-0、0-1、0-2、0-3、0-4四个分区从zk上看只对应一个消费者节点，但是由于应用是集群部署的，并且注册到zk时名字都为"meta-example_consumer1"，所以实际上0-0、0-1、0-2、0-3、0-4四个分区看起来只被"meta-example_consumer1"消费，但其实consumer1对应的却是多个消费者节点。

