![Logo](http://photo.yupoo.com/killme2008/CLRQoBA9/medish.jpg)

## Metamorphosis介绍
Metamorphosis是一个高性能、高可用、可扩展的分布式消息中间件，思路起源于LinkedIn的Kafka，但并不是Kafka的一个Copy。具有消息存储顺序写、吞吐量大和支持本地和XA事务等特性，适用于大吞吐量、顺序消息、广播和日志数据传输等场景，目前在淘宝和支付宝有着广泛的应用。

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

###总体结构
![Logo](MetaQ总体结构.png)

###内部结构
![Logo](MetaQ内部结构.png)

###Broker增加或减少时
当broker server增加或减少时，client会重新进行负载均衡。Broker减少的瞬间，在负载均衡之前，已经发送到减少的那台broker但未到达服务器时，客户端将会捕获到发送异常，由业务决定如何处理，负载均衡之后将正常发送到其他服务器上。

###客户端使用例子说明

metamorphosis-example里面有详细的使用例子，包括：
* 普通发送消息
* 异步发送消息
* 异步单向发送消息
* 本地事务发送消息
* XA事务发送消息
* Log4j发送，log4j appender配置在main/resources目录下

* 普通消费
* 广播消费
* 批量事务消费
* 同步拉取消费

客户端依赖：
```
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


什么场景下适合使用异步单向和log4j发送
对于发送可靠性要求不那么高,但要求提高发送效率和降低对宿主应用的影响，提高宿主应用的稳定性.
不在乎发送结果成功与否。
从逻辑和耗时上几乎不对业务系统产生影响




##工程结构
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





##配置参数说明

| 配置项 | 说明 | 可选值 |
|-------|------|--------|

##文件删除策略
1、超过一定时间的删除策略
2、消息归档策略


