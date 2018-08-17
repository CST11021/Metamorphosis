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
package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * ����ѡ����,�����߷�����Ϣʱ��ʹ�ø÷���ѡ����ѡ��һ������������Ϣ��Ĭ��ʹ��ѭ���ķ�ʽѡ�����
 *
 * ��������ͨ��zk��ȡ�����б�֮�󣬻ᰴ��brokerId�ͷ����ŵ�˳��������֯��һ������ķ����б����͵�ʱ���մ�ͷ��βѭ�������ķ�ʽѡ��һ��
 * ������������Ϣ������Ĭ�ϵķ������ԣ����ǵ����ǵ�broker��������Ӳ�����û���һ�£�Ĭ�ϵ���ѯ������Ȼ�㹻���������ʵ���Լ��ĸ��ؾ�����ԣ�
 * �����Լ�ʵ��PartitionSelector�ӿڣ����ڴ���producer��ʱ���뼴�ɡ�
 *
 * ��broker��Ϊ�������߹��ϵ������޷������ʱ��producerͨ��zookeeper���֪������仯����ʧЧ�ķ������б����Ƴ�����fail over��
 * ��Ϊ�ӹ��ϵ���֪�仯��һ���ӳ٣���������һ˲����в��ֵ���Ϣ����ʧ�ܡ�
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public interface PartitionSelector {

    /**
     * ����topic��message��partitions�б���ѡ�����
     * 
     * @param topic         ��ʾ����Ϣ����������
     * @param partitions    ��ʾ��ѡ��ķ����б�
     * @param message       ��Ϣ����
     *
     * @return
     * @throws MetaClientException �˷����׳����κ��쳣��Ӧ����װΪMetaClientException
     */
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
}