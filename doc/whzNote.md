![Logo](http://photo.yupoo.com/killme2008/CLRQoBA9/medish.jpg)

## Metamorphosis����
Metamorphosis��һ�������ܡ��߿��á�����չ�ķֲ�ʽ��Ϣ�м����˼·��Դ��LinkedIn��Kafka����������Kafka��һ��Copy��������Ϣ�洢˳��д�����������֧�ֱ��غ�XA��������ԣ������ڴ���������˳����Ϣ���㲥����־���ݴ���ȳ�����Ŀǰ���Ա���֧�������Ź㷺��Ӧ�á�

###����
* �����ߡ��������������߶��ɷֲ�
* ��Ϣ�洢˳��д
* ���ܼ���,��������
* ֧����Ϣ˳��
* ֧�ֱ��غ�XA����
* �ͻ���pull�������,����sendfileϵͳ���ã�zero-copy ,����������
* ֧�����Ѷ�����
* ֧����Ϣ�㲥ģʽ
* ֧���첽������Ϣ
* ֧��httpЭ��
* ֧����Ϣ���Ժ�recover
* ����Ǩ�ơ����ݶ��û�͸��
* ����״̬�����ڿͻ���
* ֧��ͬ�����첽��������HA
* ֧��group commit
* ���࡭��

###����ṹ
![Logo](MetaQ����ṹ.png)

###�ڲ��ṹ
![Logo](MetaQ�ڲ��ṹ.png)

###Broker���ӻ����ʱ
��broker server���ӻ����ʱ��client�����½��и��ؾ��⡣Broker���ٵ�˲�䣬�ڸ��ؾ���֮ǰ���Ѿ����͵����ٵ���̨broker��δ���������ʱ���ͻ��˽��Ჶ�񵽷����쳣����ҵ�������δ������ؾ���֮���������͵������������ϡ�

###�ͻ���ʹ������˵��

metamorphosis-example��������ϸ��ʹ�����ӣ�������
* ��ͨ������Ϣ
* �첽������Ϣ
* �첽��������Ϣ
* ������������Ϣ
* XA��������Ϣ
* Log4j���ͣ�log4j appender������main/resourcesĿ¼��

* ��ͨ����
* �㲥����
* ������������
* ͬ����ȡ����

�ͻ���������
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
�������д��������Ƿ����Լ���maven�ⷢ�����ͻ�����


ʲô�������ʺ�ʹ���첽�����log4j����
���ڷ��Ϳɿ���Ҫ����ô��,��Ҫ����߷���Ч�ʺͽ��Ͷ�����Ӧ�õ�Ӱ�죬�������Ӧ�õ��ȶ���.
���ں����ͽ���ɹ����
���߼��ͺ�ʱ�ϼ�������ҵ��ϵͳ����Ӱ��




##���̽ṹ
* Client,�����ߺ������߿ͻ���
* Client-extension����չ�Ŀͻ��ˡ����ڽ����Ѵ���ʧ�ܵ���Ϣ����notify(δ�ṩ),��ʹ��meta��Ϊlog4j appender������͸����ʹ��log4j API������Ϣ��meta��
* Commons���ͻ��˺ͷ����һЩ���õĶ���
* Example,�ͻ���ʹ�õ�����
* http-client��ʹ��httpЭ��Ŀͻ���
* server������˹���
* server-wrapper����չ�ķ���ˣ����ڽ�����������ɵ�����ˣ��ṩ��չ����
    1.Meta gergor�����ڸ߿��õ�ͬ������
    2.Meta slave�����ڸ߿��õ��첽����
    3.http���ṩhttpЭ��֧��
* Meta spout�����ڽ�meta��Ϣ���뵽twitter storm��Ⱥ��ʵʱ����
* Tools���ṩ����˹���Ͳ�����һЩ����





##���ò���˵��

| ������ | ˵�� | ��ѡֵ |
|-------|------|--------|

##�ļ�ɾ������
1������һ��ʱ���ɾ������
2����Ϣ�鵵����


