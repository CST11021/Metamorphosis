package com.taobao.metamorphosis.server.filter;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.StringUtils;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.ThreadUtils;


/**
 * Consumer filter manager.
 *
 * metaq��Ϣ��������ʹ��
 * ��Ϣ���������������Ѷ�ʹ��Ҳ�����ڷ����ʹ�ã������ʹ�õ�ʵ�ֻ����ǣ��������ߴ�MQ��ȡ��Ϣʱ������˻���ݹ�������������Ϣ������˹�����Ϣ��Ҫ�Լ�ʵ����Ϣ�������������jar����meta��װĿ¼�£�Ȼ����topic���������ö�Ӧ�Ĺ������������������ã�������ֱ����metaԴ���������һ��������
 * [system]
 * appClassPath=/Users/wanghongzhan/.m2/repository/com/taobao/metamorphosis/metamorphosis-server
 *
 * [topic=meta-test]
 * group.meta-example=com.taobao.metamorphosis.server.filter.ExampleConsumerMessageFilter
 * 
 * @author dennis<killme2008@gmail.com>
 * 
 */
public class ConsumerFilterManager implements Service {

    private static final Log log = LogFactory.getLog(ConsumerFilterManager.class);

    private ClassLoader filterClassLoader;

    /** meta���� */
    private MetaConfig metaConfig;
    /** Map<className,  FutureTask<ConsumerMessageFilter>>*/
    private final ConcurrentHashMap<String, FutureTask<ConsumerMessageFilter>> filters = new ConcurrentHashMap<String, FutureTask<ConsumerMessageFilter>>();


    public ConsumerFilterManager() {

    }
    public ConsumerFilterManager(MetaConfig metaConfig) throws Exception {
        this.metaConfig = metaConfig;
        if (!StringUtils.isBlank(metaConfig.getAppClassPath())) {
            File dir = new File(metaConfig.getAppClassPath());
            File[] jars = dir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".jar");
                }
            });
            URL[] urls = new URL[jars.length + 1];
            urls[0] = dir.toURI().toURL();
            int i = 1;
            for (File jarFile : jars) {
                urls[i++] = jarFile.toURI().toURL();
            }
            this.filterClassLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        }
    }


    /**
     * ����topic��group��ȡһ����Ӧ����Ϣ������
     *
     * @param topic
     * @param group
     * @return
     */
    public ConsumerMessageFilter findFilter(final String topic, final String group) {
        if (this.filterClassLoader == null) {
            return null;
        }
        final String className = this.metaConfig.getTopicConfig(topic).getFilterClass(group);
        if (StringUtils.isBlank(className)) {
            return null;
        }

        FutureTask<ConsumerMessageFilter> task = this.filters.get(className);
        if (task == null) {
            task = new FutureTask<ConsumerMessageFilter>(new Callable<ConsumerMessageFilter>() {

                @Override
                public ConsumerMessageFilter call() throws Exception {
                    ConsumerMessageFilter intanceFilter = ConsumerFilterManager.this.intanceFilter(className);
                    if (intanceFilter != null) {
                        log.warn("Created filter '" + className + "' for group:" + group + " and topic:" + topic);
                    }
                    return intanceFilter;
                }

            });

            // putIfAbsent������
            //      �������key��Ӧ��value�Ѿ����ڣ��ͷ��ش��ڵ�value���������滻��
            //      ��������ڣ������key��value������null��
            FutureTask<ConsumerMessageFilter> existsTask = this.filters.putIfAbsent(className, task);
            if (existsTask != null) {
                task = existsTask;
            } else {
                task.run();
            }
        }
        return this.getFilter0(task);
    }

    /**
     * ʹ�������������һ����Ϣ������
     *
     * @param className
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private ConsumerMessageFilter intanceFilter(String className) throws Exception {
        Class<ConsumerMessageFilter> clazz =
                (Class<ConsumerMessageFilter>) Class.forName(className, true, this.filterClassLoader);
        if (clazz != null) {
            return clazz.newInstance();
        }
        else {
            return null;
        }
    }

    /**
     * ��ȡ��Ϣ����������
     *
     * @param task
     * @return
     */
    private ConsumerMessageFilter getFilter0(FutureTask<ConsumerMessageFilter> task) {
        try {
            return task.get();
        }
        catch (ExecutionException e) {
            throw ThreadUtils.launderThrowable(e.getCause());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void dispose() {
        this.filterClassLoader = null;
        this.filters.clear();
    }

    ClassLoader getFilterClassLoader() {
        return this.filterClassLoader;
    }
    void setFilterClassLoader(ClassLoader filterClassLoader) {
        this.filterClassLoader = filterClassLoader;
    }

}
