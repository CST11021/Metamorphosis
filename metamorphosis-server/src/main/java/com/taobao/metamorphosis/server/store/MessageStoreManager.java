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
package com.taobao.metamorphosis.server.store;

import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.exception.IllegalTopicException;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.server.exception.ServiceStartupException;
import com.taobao.metamorphosis.server.exception.WrongPartitionException;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.ThreadUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.DirectSchedulerFactory;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * ��Ϣ�洢��������MQ����Ϣ�Ǳ��浽���̵ģ��ù�������������̵�IO������
 * 1������Ϣ��������MQ������Ϣʱ��ͨ���ù��������浽���̣�
 * 2������Ϣ�����ߴ�MQ��ȡ��Ϣ��������ʱ��ͨ���ù������Ӵ��̻�ȡ��Ϣ���ڷ��͸�������
 *
 * 
 * @author boyan
 * @Date 2011-4-21
 * @author wuhua
 * @Date 2011-6-26
 */
public class MessageStoreManager implements Service {

    static final Log log = LogFactory.getLog(MessageStoreManager.class);

    public static final int HALF_DAY = 1000 * 60 * 60 * 12;

    /**
     * ����Ϣ���浽���̵������߳�
     */
    private final class FlushRunner implements Runnable {
        /** ������ٺ��붨�ڽ���Ϣ���浽����, Ĭ��������10�롣Ҳ����˵�ڷ�������������£���ඪʧ10���ڷ��͹�������Ϣ��*/
        int unflushInterval;

        FlushRunner(final int unflushInterval) {
            this.unflushInterval = unflushInterval;
        }

        @Override
        public void run() {
            for (final ConcurrentHashMap<Integer, MessageStore> map : MessageStoreManager.this.stores.values()) {
                for (final MessageStore store : map.values()) {
                    // �ж����topic�Ľ���Ϣflush�����̵�ʱ�����Ƿ�Ϊ��ʱ����ִ�е�ʱ����
                    if (this.unflushInterval != MessageStoreManager.this.metaConfig.getTopicConfig(store.getTopic())
                            .getUnflushInterval()) {
                        continue;
                    }

                    try {
                        store.flush();
                    }
                    catch (final IOException e) {
                        log.error("Try to flush store failed", e);
                    }
                }
            }
        }
    }

    /** Map<topic, Map<partition, MessageStore>> ���ڴ洢��Ϣ */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, MessageStore>> stores = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, MessageStore>>();
    /** MQ������� */
    private final MetaConfig metaConfig;

    /** ��ʱ����Ϣ���浽���̵��̳߳� */
    private ScheduledThreadPoolExecutor scheduledExecutorService;

    /** ��Ϣ�ļ���ɾ�����ԣ�����Ϣ��MQ����˱���̫��һֱû�б�����ʱ��ͨ���ò��Դ�MQ���Ƴ� */
    private final DeletePolicy deletePolicy;

    /** ���ھ���ָ����topic����Ϣ�ļ�ɾ�����Ե�ѡ���� */
    private DeletePolicySelector deletePolicySelector;

    /** topic������У�飬ֻ��ƥ���˼����е�������ʽ���ǺϷ���topic */
    private final Set<Pattern> topicsPatSet = new HashSet<Pattern>();

    private final ConcurrentHashMap<Integer, ScheduledFuture<?>> unflushIntervalMap = new ConcurrentHashMap<Integer, ScheduledFuture<?>>();

    /** ��ʾ���ڶ�ʱɾ����Ϣ�ļ�������ִ���� */
    private Scheduler scheduler;

    private final Random random = new Random();

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

    /**
     * ����flushʱ�������࣬�ֱ��ύ��ʱ���񣨽���Ϣ���浽���̵�����
     */
    private void scheduleFlushTask() {
        log.info("Begin schedule flush task...");
        final Set<Integer> newUnflushIntervals = new HashSet<Integer>();
        for (final String topic : this.metaConfig.getTopics()) {
            newUnflushIntervals.add(this.metaConfig.getTopicConfig(topic).getUnflushInterval());
        }

        // �����̳߳ش�С
        if (newUnflushIntervals.size() != this.unflushIntervalMap.size()) {
            this.scheduledExecutorService.setCorePoolSize(newUnflushIntervals.size() + 1);
        }

        // �µ��У��ɵ�û�У��ύ����
        for (final Integer unflushInterval : newUnflushIntervals) {
            if (!this.unflushIntervalMap.containsKey(unflushInterval) && unflushInterval > 0) {
                final ScheduledFuture<?> future = this.scheduledExecutorService.scheduleAtFixedRate(
                        new FlushRunner(unflushInterval), unflushInterval, unflushInterval, TimeUnit.MILLISECONDS);
                this.unflushIntervalMap.put(unflushInterval, future);
                log.info("Create flush task,unflushInterval=" + unflushInterval);
            }
        }

        // �µ�û�У��ɵ��У���������
        final Set<Integer> set = new HashSet<Integer>(this.unflushIntervalMap.keySet());
        for (final Integer unflushInterval : set) {
            if (!newUnflushIntervals.contains(unflushInterval)) {
                final ScheduledFuture<?> future = this.unflushIntervalMap.remove(unflushInterval);
                if (future != null) {
                    future.cancel(false);
                    log.info("Cancel flush task,unflushInterval=" + unflushInterval);
                }
            }
        }

        this.scheduledExecutorService.purge();
        log.info("Schedule flush task finished. CorePoolSize=" + this.scheduledExecutorService.getCorePoolSize()
            + ",current pool size=" + this.scheduledExecutorService.getPoolSize());
    }

    /** ��ʼ����ʱ�̳߳� */
    private void initScheduler() {
        // ���ݶ�ʱflushʱ��������,���㶨ʱ�̳߳ش�С,����ʼ��
        final Set<Integer> tmpSet = new HashSet<Integer>();
        for (final String topic : this.metaConfig.getTopics()) {
            final int unflushInterval = this.metaConfig.getTopicConfig(topic).getUnflushInterval();
            tmpSet.add(unflushInterval);
        }
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(tmpSet.size() + 5);

        try {
            if (DirectSchedulerFactory.getInstance().getAllSchedulers().isEmpty()) {
                DirectSchedulerFactory.getInstance().createVolatileScheduler(this.metaConfig.getQuartzThreadCount());
            }
            this.scheduler = DirectSchedulerFactory.getInstance().getScheduler();
        }
        catch (final SchedulerException e) {
            throw new ServiceStartupException("Initialize quartz scheduler failed", e);
        }
    }

    /**
     * ����������Ϣ��������Ϣ�ļ�ɾ�����Ե�ѡ��������
     */
    private void newDeletePolicySelector() {
        this.deletePolicySelector = new DeletePolicySelector(this.metaConfig);
    }

    /**
     * ����У��topic�Ϸ��Ե�������ʽ
     */
    private void makeTopicsPatSet() {
        for (String topic : this.metaConfig.getTopics()) {
            topic = topic.replaceAll("\\*", ".*");
            this.topicsPatSet.add(Pattern.compile(topic));
        }
    }

    /**
     * topic���ļ�ɾ������ѡ����
     * @author wuhua
     */
    static class DeletePolicySelector {

        /** Map<topic, DeletePolicy> */
        private final Map<String, DeletePolicy> deletePolicyMap = new HashMap<String, DeletePolicy>();

        DeletePolicySelector(final MetaConfig metaConfig) {
            for (final String topic : metaConfig.getTopics()) {
                final TopicConfig topicConfig = metaConfig.getTopicConfig(topic);
                // ���topicConfigû�����ã���ʹ��ȫ������
                final String deletePolicy = topicConfig != null ? topicConfig.getDeletePolicy() : metaConfig.getDeletePolicy();
                this.deletePolicyMap.put(topic, DeletePolicyFactory.getDeletePolicy(deletePolicy));
            }
        }

        /**
         * ��ȡָ��topic���ļ�ɾ������
         * @param topic             topic����
         * @param defaultPolicy     Ĭ�ϲ���
         * @return
         */
        DeletePolicy select(final String topic, final DeletePolicy defaultPolicy) {
            final DeletePolicy deletePolicy = this.deletePolicyMap.get(topic);
            return deletePolicy != null ? deletePolicy : defaultPolicy;
        }
    }

    /** Map<topic, Map<partition, MessageStore>> */
    public Map<String, ConcurrentHashMap<Integer, MessageStore>> getMessageStores() {
        return Collections.unmodifiableMap(this.stores);
    }

    /** ͳ����Ϣ���� */
    public long getTotalMessagesCount() {
        long rt = 0;
        for (final ConcurrentHashMap<Integer/* partition */, MessageStore> subMap : MessageStoreManager.this.stores
                .values()) {
            if (subMap != null) {
                for (final MessageStore msgStore : subMap.values()) {
                    if (msgStore != null) {
                        rt += msgStore.getMessageCount();
                    }
                }
            }
        }
        return rt;
    }

    /** ͳ��topic���� */
    public int getTopicCount() {
        List<String> topics = this.metaConfig.getTopics();
        int count = this.stores.size();
        for (String topic : topics) {
            if (!this.stores.containsKey(topic)) {
                count++;
            }
        }
        return count;
    }

    /**
     * �������÷���������Ϣ�洢�ڴ��̵�Ŀ¼����ͬ��topic����ͨ�ķ������ж�Ӧ��Ŀ¼
     * @param metaConfig
     * @return
     * @throws IOException
     */
    private Set<File> getDataDirSet(final MetaConfig metaConfig) throws IOException {
        final Set<String> paths = new HashSet<String>();
        // ��������Ϣ�洢·��
        paths.add(metaConfig.getDataPath());

        // topic data path
        for (final String topic : metaConfig.getTopics()) {
            final TopicConfig topicConfig = metaConfig.getTopicConfig(topic);
            if (topicConfig != null) {
                paths.add(topicConfig.getDataPath());
            }
        }
        final Set<File> fileSet = new HashSet<File>();
        for (final String path : paths) {
            fileSet.add(this.getDataDir(path));
        }
        return fileSet;
    }

    /**
     * ����Ϣ�������е���Ϣ���浽���ش���
     * @param metaConfig
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadMessageStores(final MetaConfig metaConfig) throws IOException, InterruptedException {
        for (final File dir : this.getDataDirSet(metaConfig)) {
            this.loadDataDir(metaConfig, dir);
        }
    }

    /**
     * ��MQ�ڴ��е���Ϣ���浽���ش��̵�ָ��Ŀ¼��ÿ��topic��Ӧ�ķ���������Ӧ��Ŀ¼��������
     *
     * @param metaConfig    ������Ϣ����ȡ��Ϣ����ʱ��Ϊ����
     * @param dir           topic��Ӧ��Ϣ������Ŀ¼
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadDataDir(final MetaConfig metaConfig, final File dir) throws IOException, InterruptedException {
        log.warn("Begin to scan data path:" + dir.getAbsolutePath());
        final long start = System.currentTimeMillis();

        final File[] ls = dir.listFiles();
        // java.lang.Runtime.availableProcessors() ����: ���ؿ��ô�������Java�����������
        int nThreads = Runtime.getRuntime().availableProcessors() + 2;
        // ���̳߳����ڽ�MQ�е���Ϣ�洢������
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);

        int count = 0;
        List<Callable<MessageStore>> tasks = new ArrayList<Callable<MessageStore>>();
        for (final File subDir : ls) {
            count++;

            if (!subDir.isDirectory()) {
                log.warn("Ignore not directory path:" + subDir.getAbsolutePath());
            }
            else {
                // topic��Ϣ�����Ŀ¼��subDir��, subDirĿ¼�»��з���Ŀ¼
                final String name = subDir.getName();
                // ��ȡ��������
                final int index = name.lastIndexOf('-');
                if (index < 0) {
                    log.warn("Ignore invlaid directory:" + subDir.getAbsolutePath());
                    continue;
                }

                // ������񣺽�MQ�ڴ��е���Ϣ���浽Ӳ�̵�����
                tasks.add(new Callable<MessageStore>() {
                    @Override
                    public MessageStore call() throws Exception {
                        log.warn("Loading data directory:" + subDir.getAbsolutePath() + "...");
                        final String topic = name.substring(0, index);
                        // ��ȡtopic�µķ���
                        final int partition = Integer.parseInt(name.substring(index + 1));
                        // ��ʾһ����������Ϣ
                        final MessageStore messageStore = new MessageStore(
                                topic,
                                partition,
                                metaConfig,
                                MessageStoreManager.this.deletePolicySelector.select(topic, MessageStoreManager.this.deletePolicy));
                        return messageStore;
                    }
                });

                if (count % nThreads == 0 || count == ls.length) {
                    // ���д洢������
                    if (metaConfig.isLoadMessageStoresInParallel()) {
                        this.loadStoresInParallel(executor, tasks);
                    }
                    else {
                        this.loadStores(tasks);
                    }
                    tasks.clear();
                }
            }
        }
        // �洢��ɺ��̳߳عر�
        executor.shutdownNow();
        log.warn("End to scan data path in " + (System.currentTimeMillis() - start) / 1000 + " secs");
    }

    /**
     * �Դ��еķ�ʽ������Ϣ�洢
     * @param tasks
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadStores(List<Callable<MessageStore>> tasks) throws IOException, InterruptedException {
        for (Callable<MessageStore> task : tasks) {
            MessageStore messageStore;
            try {
                messageStore = task.call();
                ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(messageStore.getTopic());
                if (map == null) {
                    map = new ConcurrentHashMap<Integer, MessageStore>();
                    this.stores.put(messageStore.getTopic(), map);
                }
                map.put(messageStore.getPartition(), messageStore);
            }
            catch (IOException e) {
                throw e;
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * �Բ��еķ�ʽ������Ϣ�洢
     * @param executor  ���д���ʱʹ�õ��̳߳ط���
     * @param tasks     ��ʾ��Ϣ�洢������
     * @throws InterruptedException
     */
    private void loadStoresInParallel(ExecutorService executor, List<Callable<MessageStore>> tasks) throws InterruptedException {
        CompletionService<MessageStore> completionService = new ExecutorCompletionService<MessageStore>(executor);
        for (Callable<MessageStore> task : tasks) {
            completionService.submit(task);
        }
        for (int i = 0; i < tasks.size(); i++) {
            try {
                MessageStore messageStore = completionService.take().get();

                ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(messageStore.getTopic());
                if (map == null) {
                    map = new ConcurrentHashMap<Integer, MessageStore>();
                    this.stores.put(messageStore.getTopic(), map);
                }
                map.put(messageStore.getPartition(), messageStore);
            }
            catch (ExecutionException e) {
                throw ThreadUtils.launderThrowable(e);
            }
        }
    }

    /**
     * ����·������һ��File����
     * @param path
     * @return
     * @throws IOException
     */
    private File getDataDir(final String path) throws IOException {
        final File dir = new File(path);
        if (!dir.exists() && !dir.mkdir()) {
            throw new IOException("Could not make data directory " + dir.getAbsolutePath());
        }
        if (!dir.isDirectory() || !dir.canRead()) {
            throw new IOException("Data path " + dir.getAbsolutePath() + " is not a readable directory");
        }
        return dir;
    }

    /**
     * ����ָ��topic��һ���������
     * @param topic
     * @return
     */
    public int chooseRandomPartition(final String topic) {
        return this.random.nextInt(this.getNumPartitions(topic));
    }

    /**
     * ��ȡtopic�ķ�����
     * @param topic
     * @return
     */
    public int getNumPartitions(final String topic) {
        final TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
        return topicConfig != null ? topicConfig.getNumPartitions() : this.metaConfig.getNumPartitions();
    }

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

    @Override
    public void init() {
        // �����������ݲ�У��
        try {
            this.loadMessageStores(this.metaConfig);
        }
        catch (final IOException e) {
            log.error("load message stores failed", e);
            throw new MetamorphosisServerStartupException("Initilize message store manager failed", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ������ʱɾ����Ϣ������ִ����
        this.startScheduleDeleteJobs();
    }

    /**
     * ������ʱɾ����Ϣ�ļ�������ִ����
     */
    private void rescheduleDeleteJobs() {
        if (this.scheduler != null) {
            try {
                log.info("Begin clear delete jobs...");
                scheduler.clear();

                startScheduleDeleteJobs();
                log.info("Reschedule delete jobs successful !");
            } catch (final SchedulerException e) {
                log.error("Reschedule delete jobs failed", e);
            }
        }
    }

    /**
     * ������ʱɾ����Ϣ�ļ�������ִ����
     */
    private void startScheduleDeleteJobs() {
        // ��ʾ��ʲôʱ��ִ��ʲô����
        final Map<String/* deleteWhen */, JobInfo> jobs = new HashMap<String, MessageStoreManager.JobInfo>();

        // 1������ִ������
        for (final String topic : this.getAllTopics()) {
            final TopicConfig topicConfig = this.metaConfig.getTopicConfig(topic);
            final String deleteWhen =
                    topicConfig != null ? topicConfig.getDeleteWhen() : this.metaConfig.getDeleteWhen();
                    JobInfo jobInfo = jobs.get(deleteWhen);
                    if (jobInfo == null) {

                        final JobDetail job = newJob(DeleteJob.class).build();
                        job.getJobDataMap().put(DeleteJob.TOPICS, new HashSet<String>());
                        job.getJobDataMap().put(DeleteJob.STORE_MGR, this);
                        final Trigger trigger = newTrigger().withSchedule(cronSchedule(deleteWhen)).forJob(job).build();
                        jobInfo = new JobInfo(job, trigger);
                        jobs.put(deleteWhen, jobInfo);

                    }
                    // ��ӱ�topic
                    ((Set<String>) jobInfo.job.getJobDataMap().get(DeleteJob.TOPICS)).add(topic);
        }

        // 2�����������б�������ʱ��������quartz job��
        for (final JobInfo jobInfo : jobs.values()) {
            try {
                this.scheduler.scheduleJob(jobInfo.job, jobInfo.trigger);
            }
            catch (final SchedulerException e) {
                throw new ServiceStartupException("Schedule delete job failed", e);
            }
        }
        try {
            this.scheduler.start();
        }
        catch (final SchedulerException e) {
            throw new ServiceStartupException("Start scheduler failed", e);
        }
    }

    /**
     * ���ڷ�װɾ����Ϣ�ļ���������Ϣ
     */
    private static class JobInfo {

        public final JobDetail job;
        public final Trigger trigger;

        public JobInfo(final JobDetail job, final Trigger trigger) {
            super();
            this.job = job;
            this.trigger = trigger;
        }

    }

    /**
     * ��ȡ���е�topic:
     * 1�������ļ��е�����topic
     * 2����Ϣ��ϵ�е�topic
     * @return
     */
    public Set<String> getAllTopics() {
        final Set<String> rt = new TreeSet<String>();
        rt.addAll(this.metaConfig.getTopics());
        rt.addAll(this.getMessageStores().keySet());
        return rt;
    }

    /**
     * ����topic��partition��ȡһ������ʵ�������û���򷵻�null
     * @param topic
     * @param partition
     * @return
     */
    public MessageStore getMessageStore(final String topic, final int partition) {
        final ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(partition);
    }

    /**
     * ��ȡtopic�����з���ʵ��
     * @param topic
     * @return
     */
    Collection<MessageStore> getMessageStoresByTopic(final String topic) {
        final ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            return Collections.emptyList();
        }
        return map.values();
    }

    /**
     * ����topic��partition��ȡ�򴴽�һ��������Ϣ�ķ���ʵ��
     * @param topic
     * @param partition
     * @return
     * @throws IOException
     */
    public MessageStore getOrCreateMessageStore(final String topic, final int partition) throws IOException {
        return this.getOrCreateMessageStoreInner(topic, partition, 0);
    }

    /**
     * ����topic��partition��ȡ�򴴽�һ��������Ϣ�ķ���ʵ��
     * @param topic             topic
     * @param partition         ����
     * @param offsetIfCreate    ��ʾ��ǰ��������Ϣ��ƫ������ֻ���´����ķ�����Ч
     * @return
     * @throws IOException
     */
    public MessageStore getOrCreateMessageStore(final String topic, final int partition, final long offsetIfCreate) throws IOException {
        return this.getOrCreateMessageStoreInner(topic, partition, offsetIfCreate);
    }

    /**
     * ��ȡ�򴴽�һ��topic�µ�ָ��������Ӧ�ķ���ʵ��
     * @param topic             topic
     * @param partition         ����
     * @param offsetIfCreate    ��ʾ��ǰ��������Ϣ��ƫ������ֻ���´����ķ�����Ч
     * @return
     * @throws IOException
     */
    private MessageStore getOrCreateMessageStoreInner(final String topic, final int partition, final long offsetIfCreate) throws IOException {
        // �ж��ǲ��ǺϷ���topic
        if (!this.isLegalTopic(topic)) {
            throw new IllegalTopicException("The server do not accept topic " + topic);
        }

        if (partition < 0 || partition >= this.getNumPartitions(topic)) {
            log.warn("Wrong partition " + partition + ",valid partitions (0," + (this.getNumPartitions(topic) - 1) + ")");
            throw new WrongPartitionException("wrong partition " + partition);
        }

        // ��ȡtopic�·�����Ӧ�ķ���ʵ����
        ConcurrentHashMap<Integer/* partition */, MessageStore> map = this.stores.get(topic);
        if (map == null) {
            map = new ConcurrentHashMap<Integer, MessageStore>();
            final ConcurrentHashMap<Integer/* partition */, MessageStore> oldMap = this.stores.putIfAbsent(topic, map);
            if (oldMap != null) {
                map = oldMap;
            }
        }

        MessageStore messageStore = map.get(partition);
        if (messageStore != null) {
            return messageStore;
        }
        else {
            // ��string����������
            synchronized (topic.intern()) {
                messageStore = map.get(partition);
                // double check
                if (messageStore != null) {
                    return messageStore;
                }
                messageStore = new MessageStore(topic, partition, this.metaConfig,
                        this.deletePolicySelector.select(topic, this.deletePolicy), offsetIfCreate);
                log.info("Created a new message storage for topic=" + topic + ",partition=" + partition);
                map.put(partition, messageStore);
            }
        }
        return messageStore;
    }

    /**
     * �ж��ǲ��ǺϷ���topic
     * @param topic
     * @return
     */
    boolean isLegalTopic(final String topic) {
        for (final Pattern pat : this.topicsPatSet) {
            if (pat.matcher(topic).matches()) {
                return true;
            }
        }
        return false;

    }
}
