
#Server��������
##1.ע��ShutdownHook
###1.1����broker��zk��ע��
###1.2ֹͣ���ڴ���get��put������̳߳ع�����ExecutorsManager
˵����ExecutorsManager���������������̳߳أ�

* ���ڴ���get���󣨾��������ߴ�MQ��ȡ��Ϣ�����󣩵��̳߳�
* ���ڴ��������put���󣨾�����������MQ������Ϣ�����󣩵��̳߳�

���룺

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
            // shutdown�����̳߳�״̬��ΪSHUTDOWN,����������ֹͣ�����ø÷�����ֹͣ�����ⲿsubmit�����񣬲����ڲ������ܵ�����Ͷ�����ȴ�������ִ����󣬲�����ֹͣ
            // awaitTermination����ǰ�߳�������ֱ�����������ύ�����񣨰��������ܵĺͶ����еȴ��ģ�ִ������ߵȳ�ʱʱ�䵽
            this.getExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            this.unOrderedPutExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            // ignore
        }
    }
```

###1.3ֹͣ��Ϣ�洢������MessageStoreManager

�رն�ʱ����Ϣ���浽���̵��̳߳ط���

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
                            // �رշ���ʵ��
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
     * �ر�д��
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.closed = true;
        this.interrupt();
        // �ȴ����߳����д���첽������ʣ��δд����Ϣ
        try {
            this.join(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // �ر�segment����֤���ݶ��Ѿ��ύ������
        for (final Segment segment : this.segments.view()) {
            segment.fileMessageSet.close();
        }
    }
```

###1.4ֹͣͳ�ƹ�����StatsManager
###1.5�ر�ͨѶ��Server
###1.6�ӵ�ǰJVM���Ƴ�ShutdownHook

```
// ���ӷ���ִ����ɺ�Ҫ��JVM�Ƴ�
if (!this.runShutdownHook && this.shutdownHook != null) {
    try {
        Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
    }
    catch (Exception e) {
        // ignore
    }
}
```
###1.7ֹͣMQ������������CommandProcessor
###1.8������ط���









##2.��ʼ��ͨѶ����صķ���
MateQ��ͨѶ����õ��ǰ����gecko���
gecko������

```
<dependency>
    <groupId>com.taobao.gecko</groupId>
    <artifactId>gecko</artifactId>
    <version>1.1.5-SNAPSHOT</version>
</dependency>
```
��ʼ��ͨѶ���������룺

```
/**
 * ����������Ϣ����һ������ͨѶ�����{@link RemotingServer}����
 * @param metaConfig MQ���ö���
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




###2.1ע���������
���ݿͻ��˷���Ĳ�ͨ���MQ�������ò�ͬ���������Ӧ������ͻ��˷�����������˶�Ӧ������������£�

| �ͻ��˷�������� | ����˵�������� | ˵�� 
---------------|-----------------|-----
| GetCommand     		|	GetProcessor     |     
| PutCommand     		|	PutProcessor     |     
| OffsetCommand     	|	OffsetProcessor  |     
| HeartBeatRequestCommand|	VersionProcessor |     
| QuitCommand     		|	QuitProcessor    |     
| StatsCommand     		|	StatsProcessor   |     
| TransactionCommand	|	TransactionProcessor|       










##3.��ʼ��ExecutorsManager�̳߳ط��������

* ��ʼ�����ڴ���get���󣨾��������ߴ�MQ��ȡ��Ϣ�����󣩵��̳߳أ��߳���Ĭ��ΪCPU*10
* ��ʼ�����ڴ��������put���󣨾�����������MQ������Ϣ�����󣩵��̳߳أ��߳���Ĭ��ΪCPU*10


```
/** ���ڴ���get���󣨾��������ߴ�MQ��ȡ��Ϣ�����󣩵��̳߳� */
ThreadPoolExecutor getExecutor;

/** ���ڴ��������put���󣨾�����������MQ������Ϣ�����󣩵��̳߳� */
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






##4.����IdWorker����������Ψһ����ϢID��ȫ��Ψһ��ʱ������

��MQ����˽��յ�PutCommon����ʱ����ʹ��IdWorderΪ����Ϣ����һ��Ψһ����ϢID







##5.��ʼ��MessageStoreManager��Ϣ�洢������

* ����MetaConfig#topics��������topics�����ı�ʱ������������������³�ʼ��topic��Ч�Ե�У����򡢲���ɾ��ѡ�����Ͷ�ʱɾ����Ϣ�ļ�������ִ����
* ����MetaConfig#unflushInterval������
* ����У��topic�Ϸ��Ե�������ʽ
* ��ʼ����ʱ���񣬶�ʱ����Ϣ���浽���̣�����ʼִ�н���Ϣ���浽���̵Ķ�ʱ����

```
public MessageStoreManager(final MetaConfig metaConfig, final DeletePolicy deletePolicy) {
    super();
    this.metaConfig = metaConfig;
    this.deletePolicy = deletePolicy;
    this.newDeletePolicySelector();

    // ��topics������Ӽ�������topics�����ı�ʱ������������������³�ʼ��topic��Ч�Ե�У����򡢲���ɾ��ѡ�����Ͷ�ʱɾ����Ϣ�ļ�������ִ����
    this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

        @Override
        public void propertyChange(final PropertyChangeEvent evt) {
            MessageStoreManager.this.makeTopicsPatSet();
            MessageStoreManager.this.newDeletePolicySelector();
            MessageStoreManager.this.rescheduleDeleteJobs();
        }

    });

    // ��unflushInterval�������೤ʱ����һ����Ϣͬ�������ǽ���Ϣ���浽���̣���Ӽ���
    this.metaConfig.addPropertyChangeListener("unflushInterval", new PropertyChangeListener() {
        @Override
        public void propertyChange(final PropertyChangeEvent evt) {
            // ��ʼ����Ϣ���浽���̵�����
            MessageStoreManager.this.scheduleFlushTask();
        }
    });

    // ����У��topic�Ϸ��Ե�������ʽ
    this.makeTopicsPatSet();
    // ��ʼ����ʱ�̳߳�
    this.initScheduler();
    // ��ʼִ�н���Ϣ���浽���̵Ķ�ʱ����
    this.scheduleFlushTask();
}
```








##6.��ʼ��StatsManagerͳ�ƹ�����









##7.��ʼ��BrokerZooKeeper���û�ע�ᣨ��ע����broker��topic����Ϣ��zk��

* ��ʼ��zkClient
* ����session���ڼ���������broker��zk��session���ڲ���������ʱ����broker��topic������ע�ᣩ��zk
* ��ʼ��zk�ϱ���broker��topic��Ϣ�Ľڵ�·��

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









##8.��ʼ��JournalTransactionStore����洢����

##9.��ʼ��ConsumerFilterManager

##10.��ʼ��BrokerCommandProcessor����˵��������

##11.��ʼ��TransactionalCommandProcessor�������


##12.ע��MQServer��Mbean serverƽ̨
��MetaMorphosisBroker����ע�ᵽjava�������Mbean serverƽ̨

```
/**
 * ������oע�ᵽjava��MXBeanƽ̨
 * @param o         Ҫע�ᵽMXBeanƽ̨��Bean����
 * @param name      ע�����ı�ʶ��
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