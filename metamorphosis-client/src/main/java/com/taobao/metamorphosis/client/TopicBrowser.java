package com.taobao.metamorphosis.client;

import java.util.Iterator;
import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * ÿ��topic��Ӧһ��TopicBrowserʵ�������ڲ鿴ָ��topic�µ����з�������Ϣ
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public interface TopicBrowser extends Shutdownable {

    /**
     * ����һ�����������õ��������ڷ��ʸ�topic���µ����з�����Ϣ
     * 
     * @return
     */
    public Iterator<Message> iterator();

    /**
     * ����topic���п��õķ����б�
     * 
     * @return
     */
    public List<Partition> getPartitions();

    /**
     * ���ظ�TopicBrowserʵ����Ӧ��topic
     * 
     * @return
     */
    public String getTopic();
}
