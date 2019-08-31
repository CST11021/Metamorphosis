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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.ConcurrentHashSet;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.MessageAccessor;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.consumer.ConsumerMessageFilter;
import com.taobao.metamorphosis.consumer.MessageIterator;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.MetaStatLog;
import com.taobao.metamorphosis.utils.StatConstants;


/**
 * 用于从MQ服务端抓取消息的管理器
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-9-13
 * 
 */
public class SimpleFetchManager implements FetchManager {

    static final Log log = LogFactory.getLog(SimpleFetchManager.class);

    /** 用于标识抓取消息的管理是否关闭 */
    private volatile boolean shutdown = false;

    /** 表示抓取消息的线程，线程数取决于{@link ConsumerConfig#fetchRunnerCount}配置，默认cpu个数，该线程对象是对{@link FetchRequestRunner}的封装 */
    private Thread[] fetchThreads;

    /** 表示从MQ服务器拉取消息进行消费的线程列表，fetchThreads的每个线程任务对应这里的每个requestRunner */
    private FetchRequestRunner[] requestRunners;

    /** 统计抓取请求的次数 */
    private volatile int fetchRequestCount;

    /** 用于保存抓取消息的请求队列 */
    private FetchRequestQueue requestQueue;

    /** 消费端配置 */
    private final ConsumerConfig consumerConfig;

    private final InnerConsumer consumer;

    public static final Byte PROCESSED = (byte) 1;

    private final static int CACAHE_SIZE = Integer.parseInt(System.getProperty("metaq.consumer.message_ids.lru_cache.size", "4096"));

    /**
     * 用于缓存消息ID，当消息被消费国后，会将消息缓存起来，这样当本次请求中有一个消息消费异常时，抓取请求会重新投递，这样下次拉取到的消息可能有些已经被消费了，就不需要在重复消费了。
     * 这里使用ConcurrentLRUHashMap：（最近最少使用）缓存替换策略，缓存大小为：4096 */
    private static MessageIdCache messageIdCache = new ConcurrentLRUHashMap(CACAHE_SIZE);

    private static final ThreadLocal<TopicPartitionRegInfo> currentTopicRegInfo = new ThreadLocal<TopicPartitionRegInfo>();



    public SimpleFetchManager(final ConsumerConfig consumerConfig, final InnerConsumer consumer) {
        super();
        this.consumerConfig = consumerConfig;
        this.consumer = consumer;
    }


    @Override
    public void startFetchRunner() {
        // 保存当前从服务端拉取消息请求的数目，在停止的时候要检查
        this.fetchRequestCount = this.requestQueue.size();
        this.shutdown = false;
        // 启动抓取消息的线程，从MQ服务器抓取消息，并进行消费
        for (final Thread thread : this.fetchThreads) {
            thread.start();
        }
    }

    @Override
    public void stopFetchRunner() throws InterruptedException {
        this.shutdown = true;
        this.interruptRunners();
        // 等待所有任务结束
        if (this.requestQueue != null) {
            while (this.requestQueue.size() < this.fetchRequestCount) {
                this.interruptRunners();
            }
        }
        this.fetchRequestCount = 0;
    }

    @Override
    public void resetFetchState() {
        this.fetchRequestCount = 0;
        this.requestQueue = new FetchRequestQueue();

        this.fetchThreads = new Thread[this.consumerConfig.getFetchRunnerCount()];
        this.requestRunners = new FetchRequestRunner[this.consumerConfig.getFetchRunnerCount()];
        for (int i = 0; i < this.fetchThreads.length; i++) {
            FetchRequestRunner runner = new FetchRequestRunner();
            this.requestRunners[i] = runner;
            this.fetchThreads[i] = new Thread(runner);
            this.fetchThreads[i].setName(this.consumerConfig.getGroup() + "-fetch-Runner-" + i);
        }
    }

    @Override
    public int getFetchRequestCount() {
        return this.fetchRequestCount;
    }

    @Override
    public boolean isShutdown() {
        return this.shutdown;
    }

    @Override
    public void addFetchRequest(final FetchRequest request) {
        this.requestQueue.offer(request);

    }

    /**
     * 从请求队列中获取一个请求对象
     *
     * @return
     * @throws InterruptedException
     */
    FetchRequest takeFetchRequest() throws InterruptedException {
        return this.requestQueue.take();
    }

    /**
     * 终端所有任务
     */
    private void interruptRunners() {
        // 中断所有任务
        if (this.fetchThreads != null) {
            for (int i = 0; i < this.fetchThreads.length; i++) {
                Thread thread = this.fetchThreads[i];
                FetchRequestRunner runner = this.requestRunners[i];
                if (thread != null) {
                    runner.shutdown();
                    runner.interruptExecutor();
                    thread.interrupt();
                    try {
                        thread.join(100);
                    }
                    catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

            }
        }
    }

    /**
     * 判断同一条消息是否超过了最大的重试消费次数，默认3次，超过会跳过这条消息并调用RejectConsumptionHandler处理
     * @param request
     * @return
     */
    boolean isRetryTooMany(final FetchRequest request) {
        return request.getRetries() > this.consumerConfig.getMaxFetchRetries();
    }

    /**
     * 判断请求重新返回抓取队列的次数是否大于5（默认）次
     * @param request
     * @return
     */
    boolean isRetryTooManyForIncrease(final FetchRequest request) {
        return request.getRetries() > this.consumerConfig.getMaxIncreaseFetchDataRetries();
    }

    /**
     * 获取最大的延迟时间，当上一次没有抓取到的消息，抓取线程就会sleep，这里为设置sleep的最大时间，默认5秒，单位毫秒，测试的时候可以设置少点，不然会有消费延迟的现象
     *
     * @return
     */
    long getMaxDelayFetchTimeInMills() {
        return this.consumerConfig.getMaxDelayFetchTimeInMills();
    }

    /**
     * Set new message id cache to prevent duplicated messages for the same consumer group.
     *
     * @since 1.4.6
     * @param newCache
     */
    public static void setMessageIdCache(MessageIdCache newCache) {
        messageIdCache = newCache;
    }
    MessageIdCache getMessageIdCache() {
        return messageIdCache;
    }

    /**
     * Returns current thread processing message's TopicPartitionRegInfo.
     *
     * @since 1.4.6
     * @return
     */
    public static TopicPartitionRegInfo currentTopicRegInfo() {
        return currentTopicRegInfo.get();
    }

    /**
     * 该线程用于从MQ服务器拉取消息，并进行消费
     */
    class FetchRequestRunner implements Runnable {

        private static final int DELAY_NPARTS = 10;

        /** 用于标识抓取线程是否停止 */
        private volatile boolean stopped = false;

        private long lastLogNoConnectionTime;

        private final ConcurrentHashSet<Thread> executorThreads = new ConcurrentHashSet<Thread>();

        void shutdown() {
            this.stopped = true;
        }

        @Override
        public void run() {
            while (!this.stopped) {
                try {
                    // 从消息抓取请求的队列获取一个请求对象
                    final FetchRequest request = SimpleFetchManager.this.requestQueue.take();
                    this.processRequest(request);
                }
                catch (final InterruptedException e) {
                    // take响应中断，忽略
                }

            }
        }

        /**
         * 处理拉取消息的请求，通知对应的消息监听器，处理抓取的消息
         *
         * @param request
         */
        void processRequest(final FetchRequest request) {
            try {
                // 从MQ服务器拉取消息，一次请求拉取多个消息
                final MessageIterator iterator = SimpleFetchManager.this.consumer.fetch(request, -1, null);
                // 获取topic对应的消息监听器
                final MessageListener listener = SimpleFetchManager.this.consumer.getMessageListener(request.getTopic());
                // 获取topic对应的消息处理器
                final ConsumerMessageFilter filter = SimpleFetchManager.this.consumer.getMessageFilter(request.getTopic());
                // 通知消息监听器处理消息
                this.notifyListener(request, iterator, listener, filter, SimpleFetchManager.this.consumer.getConsumerConfig().getGroup());
            } catch (final MetaClientException e) {
                // 当消费失败时，会将该抓取的请求重新放回到队列里，然后重新发起抓取消息的请求
                this.updateDelay(request);
                this.LogAddRequest(request, e);
            } catch (final InterruptedException e) {
                this.reAddFetchRequest2Queue(request);
            } catch (final Throwable e) {
                this.updateDelay(request);
                this.LogAddRequest(request, e);
            }
        }

        /**
         * 消息消费异常时会调用该方法，该方法会添加对应的处理异常日志，并将消息重新放回抓取队列中，进行重新消费
         *
         * @param request
         * @param e
         */
        private void LogAddRequest(final FetchRequest request, final Throwable e) {
            // 注意：这里的消费失败是指获取消息的时候失败，而不是消费者处理消息的时候失败
            if (e instanceof MetaClientException
                    && e.getCause() instanceof NotifyRemotingException
                    && e.getMessage().contains("无可用连接")) {
                // 最多30秒打印一次
                final long now = System.currentTimeMillis();
                if (this.lastLogNoConnectionTime <= 0 || now - this.lastLogNoConnectionTime > 30000) {
                    log.error("获取消息失败,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
                    this.lastLogNoConnectionTime = now;
                }
            }
            else {
                log.error("获取消息失败,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
            }
            this.reAddFetchRequest2Queue(request);
        }

        /**
         * 将offset记录到zk，然后发起一个新的请求，从MA拉取后续的消息
         *
         * @param request
         * @param e
         */
        private void getOffsetAddRequest(final FetchRequest request, final InvalidMessageException e) {
            try {
                // 将提交偏移量，返回新的偏移量，即下次需要开始拉取的消息偏移量
                final long newOffset = SimpleFetchManager.this.consumer.offset(request);
                request.resetRetries();
                if (!this.stopped) {
                    request.setOffset(newOffset, request.getLastMessageId(), request.getPartitionObject().isAutoAck());
                }
            } catch (final MetaClientException ex) {
                log.error("查询offset失败,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
            } finally {
                this.reAddFetchRequest2Queue(request);
            }
        }

        public void interruptExecutor() {
            for (Thread thread : this.executorThreads) {
                if (!thread.isInterrupted()) {
                    thread.interrupt();
                }
            }
        }

        /**
         * 通知对应的消息监听器处理从服务端拉取的消息
         * @param request   表示从MQ服务器拉取消息的请求
         * @param it
         * @param listener  消息监听器，用于处理消息
         * @param filter    消息过滤器
         * @param group
         */
        private void notifyListener(final FetchRequest request, final MessageIterator it, final MessageListener listener, final ConsumerMessageFilter filter, final String group) {
            if (listener != null) {
                // 如果消费者配置了线程池任务来处理消息的话，则走线程池处理
                if (listener.getExecutor() != null) {
                    try {
                        listener.getExecutor().execute(new Runnable() {
                            @Override
                            public void run() {
                                Thread currentThread = Thread.currentThread();
                                FetchRequestRunner.this.executorThreads.add(currentThread);
                                try {
                                    FetchRequestRunner.this.receiveMessages(request, it, listener, filter, group);
                                }
                                finally {
                                    FetchRequestRunner.this.executorThreads.remove(currentThread);
                                }
                            }
                        });
                    } catch (final RejectedExecutionException e) {
                        log.error("MessageListener线程池繁忙，无法处理消息,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
                        // MessageListener线程池繁忙，无法处理消息的时候会将消息重新放回消息队列中
                        this.reAddFetchRequest2Queue(request);
                    }

                } else {
                    this.receiveMessages(request, it, listener, filter, group);
                }
            }
        }

        /**
         * 将消息抓取的请求重新添加到队列中
         *
         * @param request
         */
        private void reAddFetchRequest2Queue(final FetchRequest request) {
            SimpleFetchManager.this.addFetchRequest(request);
        }

        /**
         * 处理消息的整个流程：<br>
         * <ul>
         * <li>1.判断是否有消息可以处理，如果没有消息并且有数据递增重试次数，并判断是否需要递增maxSize</li>
         * <li>2.判断消息是否重试多次，如果超过设定次数，就跳过该消息继续往下走。跳过的消息可能在本地重试或者交给notify重投</li>
         * <li>3.进入消息处理流程，根据是否自动ack的情况进行处理:
         * <ul>
         * <li>(1)如果消息是自动ack，如果消费发生异常，则不修改offset，延迟消费等待重试</li>
         * <li>(2)如果消息是自动ack，如果消费正常，递增offset</li>
         * <li>(3)如果消息非自动ack，如果消费正常并ack，将offset修改为tmp offset，并重设tmp offset</li>
         * <li>(4)如果消息非自动ack，如果消费正常并rollback，不递增offset，重设tmp offset</li>
         * <li>(5)如果消息非自动ack，如果消费正常不ack也不rollback，不递增offset，递增tmp offset</li>
         * </ul>
         * </li>
         * </ul>
         *
         * @param request       消费抓取消息的请求
         * @param it            表示每次从服务端抓取消息的集合，一次抓取多个
         * @param listener      表示处理消息对应的监听器
         */
        private void receiveMessages(final FetchRequest request, final MessageIterator it, final MessageListener listener, final ConsumerMessageFilter filter, final String group) {
            if (it != null && it.hasNext()) {
                // 同一条消息在处理失败情况下最大重试消费次数，默认3次，超过就跳过这条消息并调用RejectConsumptionHandler处理
                if (this.processWhenRetryTooMany(request, it)) {
                    return;
                }

                final Partition partition = request.getPartitionObject();
                if (this.processReceiveMessage(request, it, listener, filter, partition, group)) {
                    return;
                }
                this.postReceiveMessage(request, it, partition);
            }
            else {
                // 尝试多次无法解析出获取的数据，可能需要增大maxSize
                if (SimpleFetchManager.this.isRetryTooManyForIncrease(request) && it != null && it.getDataLength() > 0) {
                    // 将抓取请求的maxSize扩大为原来的1倍
                    request.increaseMaxSize();
                    log.warn("警告，第" + request.getRetries() + "次无法拉取topic=" + request.getTopic() + ",partition=" + request.getPartitionObject() + "的消息，递增maxSize=" + request.getMaxSize() + " Bytes");
                }

                // 一定要判断it是否为null,否则正常的拉到结尾时(返回null)也将进行Retries记数,会导致以后再拉到消息时进入recover
                if (it != null) {
                    request.incrementRetriesAndGet();
                }

                // 延迟请求的投递时间
                this.updateDelay(request);
                // 本次没有抓取到将消息，则将请求重新返回抓取队列中，并延迟投递，因为这时可能消费者的消费能力大于生产者的生产速度
                this.reAddFetchRequest2Queue(request);
            }
        }

        /**
         * 处理当前的消息
         *
         * @param request
         * @param it
         * @param listener
         * @param partition
         * @return 返回是否需要跳过后续的处理
         */
        private boolean processReceiveMessage(final FetchRequest request, final MessageIterator it, final MessageListener listener, final ConsumerMessageFilter filter, final Partition partition, final String group) {
            int count = 0;
            List<Long> inTransactionMsgIds = new ArrayList<Long>();
            // 遍历当前抓取的所有消息
            while (it.hasNext()) {
                // 获取迁移消息的偏移量
                final int prevOffset = it.getOffset();
                try {
                    final Message msg = it.next();
                    // If the message is processed before,don't process it again.
                    // 判断这个消息之前是否已经处理过了，每次抓取请求，抓取到的消息被正常消费后，都会缓存起来，因为当本次请求中有一个消息消费异常时，
                    // 抓取请求会重新投递，这样下次拉取到的消息可能有些已经被消费了，就不需要在重复消费了
                    if (this.isProcessed(msg.getId(), group)) {
                        continue;
                    }

                    MessageAccessor.setPartition(msg, partition);
                    // 消费者的消息过滤器是否需要过滤该消息，如果不过滤则表示该消息可以消费
                    boolean accept = this.isAcceptable(request, filter, group, msg);
                    if (accept) {
                        currentTopicRegInfo.set(request.getTopicPartitionRegInfo().clone(it));
                        try {
                            // 通知监听器处理该消息
                            listener.recieveMessages(msg);
                        }
                        finally {
                            currentTopicRegInfo.remove();
                        }
                    }

                    // 判断消息是否需要回滚
                    // rollback message if it is in rollback only state.
                    if (MessageAccessor.isRollbackOnly(msg)) {
                        it.setOffset(prevOffset);
                        break;
                    }

                    // Consumer可能需要一段时间才能处理完收到的数据。如果在这个过程中，Consumer出错了，异常退出了，而数据还没有处理完成，
                    // 这段数据就丢失了。如果我们采用no-ack的方式进行确认，也就是说，每次Consumer接到数据后，而不管是否处理完成，MQ会立即把这个Message标记为完成，然后从queue中删除了。
                    // 为了保证数据不被丢失，MQ支持消息确认机制，即ack。为了保证数据能被正确处理而不仅仅是被Consumer收到，我们就不能采用no-ack或者auto-ack，我们需要手动ack(manual-ack)。
                    // 在数据处理完成后手动发送ack，这个时候Server才将Message删除。
                    if (partition.isAutoAck()) {
                        count++;
                        this.markProcessed(msg.getId(), group);
                    } else {
                        // 提交或者回滚都必须跳出循环
                        if (partition.isAcked()) {
                            count++;
                            // mark all in transaction messages were processed.
                            //
                            for (Long msgId : inTransactionMsgIds) {
                                this.markProcessed(msgId, group);
                            }
                            this.markProcessed(msg.getId(), group);
                            break;
                        }
                        else if (partition.isRollback()) {
                            break;
                        }
                        else {
                            inTransactionMsgIds.add(msg.getId());
                            // 不是提交也不是回滚，仅递增计数
                            count++;
                        }
                    }
                } catch (InterruptedException e) {
                    // Receive messages thread is interrupted
                    it.setOffset(prevOffset);
                    log.error("Process messages thread was interrupted,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
                    break;
                } catch (final InvalidMessageException e) {
                    MetaStatLog.addStat(null, StatConstants.INVALID_MSG_STAT, request.getTopic());
                    // 消息体非法，获取有效offset，重新发起查询
                    this.getOffsetAddRequest(request, e);
                    return true;
                } catch (final Throwable e) {
                    // 将指针移到上一条消息
                    it.setOffset(prevOffset);
                    log.error("Process messages failed,topic=" + request.getTopic() + ",partition=" + request.getPartition(), e);
                    // 跳出循环，处理消息异常，到此为止
                    break;
                }
            }
            MetaStatLog.addStatValue2(null, StatConstants.GET_MSG_COUNT_STAT, request.getTopic(), count);
            return false;
        }

        /**
         * 判断这个消息之前是否已经处理过了，每次抓取请求，抓取到的消息被正常消费后，都会缓存起来，因为当本次请求中有一个消息消费异常时，抓取请求会重新投递，这样下次拉取到的消息可能有些已经被消费了，就不需要在重复消费了
         *
         * @param id        消息ID
         * @param group     消费者分组
         * @return
         */
        private boolean isProcessed(final Long id, String group) {
            if (messageIdCache != null) {
                return messageIdCache.get(this.cacheKey(id, group)) != null;
            } else {
                return false;
            }
        }

        /**
         * 已经处理过的消息会缓存起来，这里是用于获取缓存消息的ID
         *
         * @param id
         * @param group
         * @return
         */
        private String cacheKey(final Long id, String group) {
            return group + id;
        }

        /**
         * 将消费过了的消息缓存起来
         *
         * @param msgId
         * @param group
         */
        private void markProcessed(final Long msgId, String group) {
            if (messageIdCache != null) {
                messageIdCache.put(this.cacheKey(msgId, group), PROCESSED);
            }
        }

        /**
         * 消费者的消息过滤器是否需要过滤该消息
         *
         * @param request   消息抓取的请求
         * @param filter    消息过滤器
         * @param group     消费者分组
         * @param msg       消息对象
         * @return
         */
        private boolean isAcceptable(final FetchRequest request, final ConsumerMessageFilter filter, final String group, final Message msg) {
            if (filter == null) {
                return true;
            }
            else {
                try {
                    return filter.accept(group, msg);
                }
                catch (Exception e) {
                    log.error("Filter message failed,topic=" + request.getTopic() + ",group=" + group + ",filterClass="
                            + filter.getClass().getCanonicalName());
                    // If accept throw exception,we think we can't accept
                    // this message.
                    return false;
                }
            }
        }

        /**
         * 判断同一抓取请求返回的消息在处理失败情况下，是否超过了最大重试消费次数，默认3次
         *
         * @param request
         * @param it
         * @return
         */
        private boolean processWhenRetryTooMany(final FetchRequest request, final MessageIterator it) {
            // 同一条消息在处理失败情况下最大重试消费次数，默认3次，超过就跳过这条消息并调用RejectConsumptionHandler处理
            if (SimpleFetchManager.this.isRetryTooMany(request)) {

                try {
                    final Message couldNotProecssMsg = it.next();
                    MessageAccessor.setPartition(couldNotProecssMsg, request.getPartitionObject());
                    MetaStatLog.addStat(null, StatConstants.SKIP_MSG_COUNT, couldNotProecssMsg.getTopic());
                    SimpleFetchManager.this.consumer.appendCouldNotProcessMessage(couldNotProecssMsg);
                }
                catch (final InvalidMessageException e) {
                    MetaStatLog.addStat(null, StatConstants.INVALID_MSG_STAT, request.getTopic());
                    // 消息体非法，获取有效offset，重新发起查询
                    this.getOffsetAddRequest(request, e);
                    return true;
                }
                catch (final Throwable t) {
                    this.LogAddRequest(request, t);
                    return true;
                }

                request.resetRetries();
                // 尝试消费很多次，依然不能正常消费这条消息，则将该消息记录到本地，然后跳过这条不能处理的消息，
                if (!this.stopped) {
                    request.setOffset(request.getOffset() + it.getOffset(), it.getPrevMessage().getId(), true);
                }
                // 强制设置延迟为0
                request.setDelay(0);
                this.reAddFetchRequest2Queue(request);
                return true;

            }
            else {
                return false;
            }
        }

        private void postReceiveMessage(final FetchRequest request, final MessageIterator it, final Partition partition) {
            // 如果offset仍然没有前进，递增重试次数
            if (it.getOffset() == 0) {
                request.incrementRetriesAndGet();
            }else {
                request.resetRetries();
            }

            // 非自动ack模式
            if (!partition.isAutoAck()) {
                // 如果是回滚,则回滚offset，再次发起请求
                if (partition.isRollback()) {
                    request.rollbackOffset();
                    partition.reset();
                    this.addRequst(request);
                }
                // 如果提交，则更新临时offset到存储
                else if (partition.isAcked()) {
                    partition.reset();
                    this.ackRequest(request, it, true);
                }
                else {
                    // 都不是，递增临时offset
                    this.ackRequest(request, it, false);
                }
            } else {
                // 自动ack模式
                this.ackRequest(request, it, true);
            }
        }

        /**
         * 添加ack请求
         *
         * @param request
         * @param it
         * @param ack
         */
        private void ackRequest(final FetchRequest request, final MessageIterator it, final boolean ack) {
            long msgId = it.getPrevMessage() != null ? it.getPrevMessage().getId() : -1;
            request.setOffset(request.getOffset() + it.getOffset(), msgId, ack);
            this.addRequst(request);
        }

        /**
         * 添加抓取请求到队列中
         *
         * @param request
         */
        private void addRequst(final FetchRequest request) {
            final long delay = this.getRetryDelay(request);
            request.setDelay(delay);
            this.reAddFetchRequest2Queue(request);
        }

        /**
         * 获取抓取请求的延迟投递时间，延迟时间为：最大延迟时间/10*重试次数
         *
         * @param request
         * @return
         */
        private long getRetryDelay(final FetchRequest request) {
            final long maxDelayFetchTimeInMills = SimpleFetchManager.this.getMaxDelayFetchTimeInMills();
            final long nPartsDelayTime = maxDelayFetchTimeInMills / DELAY_NPARTS;
            // 延迟时间为：最大延迟时间/10*重试次数
            long delay = nPartsDelayTime * request.getRetries();
            if (delay > maxDelayFetchTimeInMills) {
                delay = maxDelayFetchTimeInMills;
            }
            return delay;
        }

        /**
         * 当消费失败时，调用该方法，来重置该请求的延迟时间（消息抓取请求是存放在延迟队列里的）
         * 当消费失败时，会将该抓取的请求重新放回到队列里，然后重新发起抓取消息的请求
         *
         * @param request
         */
        private void updateDelay(final FetchRequest request) {
            final long delay = this.getNextDelay(request);
            request.setDelay(delay);
        }

        /**
         * 获取下一次的延迟时间，即什么时候才能从消息抓取请求的队列中拿出来
         *
         * @param request
         * @return
         */
        private long getNextDelay(final FetchRequest request) {
            final long maxDelayFetchTimeInMills = SimpleFetchManager.this.getMaxDelayFetchTimeInMills();
            // 每次1/10递增,最大MaxDelayFetchTimeInMills
            final long nPartsDelayTime = maxDelayFetchTimeInMills / DELAY_NPARTS;
            long delay = request.getDelay() + nPartsDelayTime;
            if (delay > maxDelayFetchTimeInMills) {
                delay = maxDelayFetchTimeInMills;
            }
            return delay;
        }

    }

}