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
package com.taobao.metamorphosis.server.transaction.store;

/**
 * ��������MBean�ӿ�
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-25
 * 
 */
public interface JournalTransactionStoreMBean {
    /**
     * ִ��checkpoint
     *
     * checkPoint���֪ʶ��
     * �����ݿ�ϵͳ�У�д��־��д�����ļ������ݿ���IO�����������ֲ������������ֲ�����д�����ļ����ڷ�ɢд��д��־�ļ���˳��д�����Ϊ�˱�
     * ֤���ݿ�����ܣ�ͨ�����ݿⶼ�Ǳ�֤���ύ(commit)���֮ǰҪ�ȱ�֤��־����д�뵽��־�ļ��У��������ݿ��򱣴������ݻ���(buffer cache)
     * ���ٲ����ڵķ���д�뵽�����ļ��С�Ҳ����˵��־д����ύ������ͬ���ģ�������д����ύ�����ǲ�ͬ���ġ������ʹ���һ�����⣬��һ������
     * �������ʱ�򲢲��ܱ�֤���������������ȫ��д�뵽�����ļ��У�������ʵ��������ʱ���Ҫʹ����־�ļ����лָ������������ݿ�ָ�������֮ǰ
     * ��״̬����֤���ݵ�һ���ԡ���������������е���Ҫ���ƣ�ͨ������ȷ�����ָ�ʱ��Щ������־Ӧ�ñ�ɨ�貢Ӧ���ڻָ���
     * 
     * @throws Exception
     */
    public void makeCheckpoint() throws Exception;


    /**
     * ���ص�ǰ��Ծ������
     * 
     * @return
     */
    public int getActiveTransactionCount();

}