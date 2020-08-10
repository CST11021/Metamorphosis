package com.taobao.metamorphosis.client;

import java.util.Iterator;
import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 每个topic对应一个TopicBrowser实例，用于查看指定topic下的所有分区和消息
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * 
 */
public interface TopicBrowser extends Shutdownable {

    /**
     * 返回一个迭代器，该迭代器用于访问该topic的下的所有分区消息
     * 
     * @return
     */
    public Iterator<Message> iterator();

    /**
     * 返回topic所有可用的分区列表
     * 
     * @return
     */
    public List<Partition> getPartitions();

    /**
     * 返回该TopicBrowser实例对应的topic
     * 
     * @return
     */
    public String getTopic();
}
