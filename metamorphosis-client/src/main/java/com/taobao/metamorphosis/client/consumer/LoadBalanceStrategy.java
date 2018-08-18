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
package com.taobao.metamorphosis.client.consumer;

import java.util.List;


/**
 * Consumer��balance����
 *
 * ˵��metaq��������balance���ԣ����ò�˵һ�·������й���Ϣ��һ��topic���Ի���Ϊn��������ÿ��������һ������ġ����ɱ�ġ�˳������Ķ��С�
 * ����һ������Ϊ��������Ϣ�����������Էֲ��ڶ�������ϴ棬�����������ڵ�̨�����洢��С���������������ƿ���һ�ֲ��жȡ�
 *
 *         �����ߵĸ��ؾ�����topic�ķ������ݽ�����أ���Ҫ���Ǽ��������
 *
 *             1�����������ڵ���������Ŀ������ܵ÷�����Ŀ��Ļ����������������߲��������ѡ�ÿ���������ÿ��������groupֻ��һ�������ߣ�ͬһ��group�Ķ��������߲��������ѡ�
 *
 *             2����������ڵ���������Ŀ�ȷ�����ĿС�����в���������Ҫ����е���Ϣ���������񡣵�������Ŀn���ڵ���group����������Ŀmʱ������n%m����������Ҫ����е�1/n����������n�㹻���ʱ�������Ϊ����ƽ�����䡣
 *
 *         �������������������ڵ������߼�Ⱥ�ĸ��ؾ���������£�
 *
 *             ��ÿ���������һ��groupֻ����һ��������
 *
 *             �����ͬһ��group����������Ŀ���ڷ�����Ŀ���������������߲���������
 *
 *             �����ͬһ��group����������ĿС�ڷ�����Ŀ�����в�����������Ҫ����е���������
 *
 *         meta�ͻ��˴��������ߵĸ��ؾ��ⷽʽ�����������б�ͷ����б�ֱ�����Ȼ������������������Ĺ��ء����ĳ�������߹��ϣ����������߻��֪����һ�仯��Ȼ�����½��и��ؾ��⣬��֤���з������������߽������ѡ�
 *
 *         Consumer��balance����ʵ����metaq���ṩ�����֣�ConsisHashStrategy��DefaultLoadBalanceStrategy��
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface LoadBalanceStrategy {

    enum Type {
        DEFAULT,
        CONSIST
    }

    /**
     * ����consumer id���Ҷ�Ӧ�ķ����б�
     * 
     * @param topic         ����topic
     * @param consumerId    ������ID����Ϣ�����ߵ�Ψһ��ʶ
     * @param curConsumers  ��ǰ���Խ�����ȡ��Ϣ���ѵ�������
     * @param curPartitions ��ǰ�ķ����б�
     * 
     * @return ���ط����б�����ǰ��������ֻ���ѴӸýӿڷ��صķ����µ���Ϣ
     */
    public List<String> getPartitions(String topic, String consumerId, final List<String> curConsumers, final List<String> curPartitions);

}