
#Server��������
##1.ע��ShutdownHook
###1.1����broker��zk��ע��
###1.2ֹͣ���ڴ���get��put������̳߳ع�����ExecutorsManager
˵����ExecutorsManager���������������̳߳أ�

* ���ڴ���get���󣨾��������ߴ�MQ��ȡ��Ϣ�����󣩵��̳߳�
* ���ڴ��������put���󣨾�����������MQ������Ϣ�����󣩵��̳߳�

###1.3ֹͣ��Ϣ�洢������MessageStoreManager
###1.4ֹͣͳ�ƹ�����StatsManager
###1.5�ر�ͨѶ��Server
###1.6�ӵ�ǰJVM���Ƴ�ShutdownHook
###1.7ֹͣMQ������������CommandProcessor
###1.8������ط���









##2.��ʼ��ͨѶ����صķ���
MateQ��ͨѶ����õ��ǰ����gecko���
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









##4.����IdWorker����������Ψһ����ϢID��ȫ��Ψһ��ʱ������

��MQ����˽��յ�PutCommon����ʱ����ʹ��IdWorderΪ����Ϣ����һ��Ψһ����ϢID







##5.��ʼ��MessageStoreManager��Ϣ�洢������

* ����MetaConfig#topics��������topics�����ı�ʱ������������������³�ʼ��topic��Ч�Ե�У����򡢲���ɾ��ѡ�����Ͷ�ʱɾ����Ϣ�ļ�������ִ����

```
// ��topics������Ӽ�������topics�����ı�ʱ������������������³�ʼ��topic��Ч�Ե�У����򡢲���ɾ��ѡ�����Ͷ�ʱɾ����Ϣ�ļ�������ִ����
this.metaConfig.addPropertyChangeListener("topics", new PropertyChangeListener() {

    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
        MessageStoreManager.this.makeTopicsPatSet();
        MessageStoreManager.this.newDeletePolicySelector();
        MessageStoreManager.this.rescheduleDeleteJobs();
    }

});
```
* ����MetaConfig#unflushInterval������

```
// ��unflushInterval�������೤ʱ����һ����Ϣͬ�������ǽ���Ϣ���浽���̣���Ӽ���
this.metaConfig.addPropertyChangeListener("unflushInterval", new PropertyChangeListener() {
    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
        // ��ʼ����Ϣ���浽���̵�����
        MessageStoreManager.this.scheduleFlushTask();
    }
});
```
* ����У��topic�Ϸ��Ե�������ʽ
* ��ʼ����ʱ���񣬶�ʱ����Ϣ���浽���̣�����ʼִ�н���Ϣ���浽���̵Ķ�ʱ����









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