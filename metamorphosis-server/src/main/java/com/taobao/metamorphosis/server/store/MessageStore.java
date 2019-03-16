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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.SystemTimer;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;

import javax.swing.text.Segment;

/**
 * {@link MessageStore} 实例代表一个分区的实例，一个topic的消息存储，内部管理多个文件(segment)
 * {@link MessageStore} 继承了Thread类，继承该类主要是为了实现异步写入方式
 *
 * 消息在broker上的每个分区都是组织成一个文件列表，消费者拉取数据需要知道数据在文件中的偏移量，这个偏移量就是所谓offset。
 * Offset是绝对偏移量，服务器会将offset转化为具体文件的相对偏移量
 * 
 * @author boyan
 * @Date 2011-4-20
 * @author wuhua
 * @Date 2011-6-26
 * 
 */
public class MessageStore extends Thread implements Closeable {

    static final Log log = LogFactory.getLog(MessageStore.class);

    /**
     * 表示一个消息文件
     *
     * MessageStore采用Segment方式组织存储，Segment包装了FileMessageSet，由FileMessageSet进行读写，MessageStore并将多个Segment
     * 进行前后衔接，衔接方式为：
     * 第一个Segment对应的消息文件命名为0.meta;
     * 第二个则命名为第一个文件的开始位置+第一个Segment的大小;
     * 图示如下(假设现在每个文件大小都为1024byte)：
     * 0.meta -> 1024.meta -> 2048.meta -> ...
     *
     * 为什么要这样进行设计呢，主要是为了提高查询效率。MessageStore将最后一个Segment变为可变Segment，因为最后一个Segment相当于文件尾，
     * 消息是有先后顺序的，必须将消息添加到最后一个Segment上。
     */
    static class Segment {
        /** 该片段代表的offset，也就是这个消息文件的文件名，不带.mate后缀（这里可以理解为消息的id，但是实际上并不是消息的id）*/
        final long start;
        /** 表示保存到磁盘的消息文件，一个消息对应一个文件 */
        final File file;
        /** 该片段的消息集合 */
        FileMessageSet fileMessageSet;

        public Segment(final long start, final File file) {
            this(start, file, true);
        }

        public Segment(final long start, final File file, final boolean mutable) {
            super();
            this.start = start;
            this.file = file;
            log.info("Created segment " + this.file.getAbsolutePath());
            try {
                final FileChannel channel = new RandomAccessFile(this.file, "rw").getChannel();

                this.fileMessageSet = new FileMessageSet(channel, 0, channel.size(), mutable);
                // // 不可变的，这里不能直接用FileMessageSet(channel, false)
                // if (mutable == true) {
                // this.fileMessageSet.setMutable(true);
                // }
            }
            catch (final IOException e) {
                log.error("初始化消息集合失败", e);
            }
        }

        /** 获取片段大小 */
        public long size() {
            return this.fileMessageSet.highWaterMark();
        }

        /** 判断offset是否在本文件内 */
        public boolean contains(final long offset) {
            if (this.size() == 0 && offset == this.start
                    || this.size() > 0 && offset >= this.start && offset <= this.start + this.size() - 1) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    /**
     * 不可变的segment list
     *
     * @author boyan
     * @Date 2011-4-20
     *
     */
    static class SegmentList {
        AtomicReference<Segment[]> contents = new AtomicReference<Segment[]>();

        public SegmentList(final Segment[] s) {
            this.contents.set(s);
        }

        public SegmentList() {
            super();
            this.contents.set(new Segment[0]);
        }

        /**
         * 将这个Segment追加到SegmentList
         *
         * @param segment
         */
        public void append(final Segment segment) {
            while (true) {
                final Segment[] curr = this.contents.get();
                final Segment[] update = new Segment[curr.length + 1];
                System.arraycopy(curr, 0, update, 0, curr.length);
                update[curr.length] = segment;
                if (this.contents.compareAndSet(curr, update)) {
                    return;
                }
            }
        }

        /**
         * 从SegmentList删除指定的Segment元素
         *
         * @param segment
         */
        public void delete(final Segment segment) {
            while (true) {
                final Segment[] curr = this.contents.get();
                int index = -1;
                for (int i = 0; i < curr.length; i++) {
                    if (curr[i] == segment) {
                        index = i;
                        break;
                    }

                }
                if (index == -1) {
                    return;
                }
                final Segment[] update = new Segment[curr.length - 1];
                // 拷贝前半段
                System.arraycopy(curr, 0, update, 0, index);
                // 拷贝后半段
                if (index + 1 < curr.length) {
                    System.arraycopy(curr, index + 1, update, index, curr.length - index - 1);
                }
                if (this.contents.compareAndSet(curr, update)) {
                    return;
                }
            }
        }

        /**
         * 获取SegmentList中的元素内容
         *
         * @return
         */
        public Segment[] view() {
            return this.contents.get();
        }

        /**
         * 获取最后一个
         *
         * @return
         */
        public Segment last() {
            final Segment[] copy = this.view();
            if (copy.length > 0) {
                return copy[copy.length - 1];
            }
            return null;
        }

        /**
         * 获取第一个
         *
         * @return
         */
        public Segment first() {
            final Segment[] copy = this.view();
            if (copy.length > 0) {
                return copy[0];
            }
            return null;
        }
    }

    private static final int ONE_M_BYTES = 512 * 1024;
    /** 表示存储消息文件的后缀 */
    private static final String FILE_SUFFIX = ".meta";
    /** 表示分区是否被关闭 */
    private volatile boolean closed = false;
    /** 从分区目录中加载消息后，会将消息保存到该列表中（初始化消息存储器时，会将磁盘的消息文件也加载进来） */
    private SegmentList segments;
    /** 表示该消息存在本地磁盘的分区目录，分区目录下有很多.meta的消息文件 */
    private final File partitionDir;
    /** 表示消息归属的topic */
    private final String topic;
    /** 表示消息归属的分区索引 */
    private final int partition;
    /** 全局MQ配置 */
    private final MetaConfig metaConfig;
    /** 由于是多文件的存储方式，消费过的消息或过期消息需要删除从而腾出空间给新消息的，默认提供归档和过期删除的方式  */
    private final DeletePolicy deletePolicy;

    private final AtomicInteger unflushed;
    /** 表示最后一次flush消息到磁盘的时间 */
    private final AtomicLong lastFlushTime;

    /** 传输给消费者的最大数据大小,默认为1M */
    private long maxTransferSize;
    /** 如果配置了异步写入，则启动异步写入线程（如果unflushThreshold<= 0，则认为启动异步写入的方式） */
    int unflushThreshold = 1000;

    /** 表示异步消息加载时的写入队列（以异步的方式从分区目录中读取消息文件），客户端用异步的方式发消息时，会将消息先保存在该队列，后续再以异步的方式写入磁盘 */
    private final LinkedTransferQueue<WriteRequest> bufferQueue = new LinkedTransferQueue<WriteRequest>();

    private volatile String desc;

    /** 校验分区目录下的消息文件时，对其进行加锁 */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     *
     * @param topic
     * @param partition
     * @param metaConfig
     * @param deletePolicy
     * @param offsetIfCreate 表示当前分区下消息的偏移量
     * @throws IOException
     */
    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig, final DeletePolicy deletePolicy, final long offsetIfCreate) throws IOException {
        this.metaConfig = metaConfig;
        this.topic = topic;
        final TopicConfig topicConfig = this.metaConfig.getTopicConfig(this.topic);

        // 1、获取消息存储在磁盘的根目录
        String dataPath = metaConfig.getDataPath();
        if (topicConfig != null) {
            dataPath = topicConfig.getDataPath();
        }
        final File parentDir = new File(dataPath);
        this.checkDir(parentDir);

        // 2、获取消息所在的分区目录
        this.partitionDir = new File(dataPath + File.separator + topic + "-" + partition);
        this.checkDir(this.partitionDir);

        this.partition = partition;
        this.unflushed = new AtomicInteger(0);
        this.lastFlushTime = new AtomicLong(SystemTimer.currentTimeMillis());
        this.unflushThreshold = topicConfig.getUnflushThreshold();
        this.deletePolicy = deletePolicy;

        // Make a copy to avoid getting it again and again.
        this.maxTransferSize = metaConfig.getMaxTransferSize();
        // 启动异步写入的时候，消息提交到磁盘的size配置，同时也是配置组写入时，消息最大长度的控制参数，如果消息长度大于该参数，则会同步写入
        this.maxTransferSize = this.maxTransferSize > ONE_M_BYTES ? ONE_M_BYTES : this.maxTransferSize;

        // Check directory and load exists segments.
        this.checkDir(this.partitionDir);
        this.loadSegments(offsetIfCreate);

        // 是否启动异步的方式，将消息写入磁盘
        if (this.useGroupCommit()) {
            this.start();
        }
    }
    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig, final DeletePolicy deletePolicy) throws IOException {
        this(topic, partition, metaConfig, deletePolicy, 0);
    }


    /**
     * 该线程方法用于将消息存储管理器中的消息保存到磁盘
     */
    @Override
    public void run() {
        // 等待force的队列
        final LinkedList<WriteRequest> toFlush = new LinkedList<WriteRequest>();
        WriteRequest req = null;
        long lastFlushPos = 0;
        Segment last = null;

        // 存储没有关闭并且线程没有被中断
        while (!this.closed && !Thread.currentThread().isInterrupted()) {
            try {

                if (last == null) {
                    // 获取最后的一个segment，将消息写入最后segment对应的文件
                    last = this.segments.last();
                    lastFlushPos = last.fileMessageSet.highWaterMark();
                }

                if (req == null) {
                    // 如果等待提交到磁盘的队列toFlush为空，则两种可能：
                    // 一、刚刚提交完，列表为空；
                    // 二、等待写入消息的队列为空，如果判断toFlush为空，则调用bufferQueue.take()方法，可以阻塞住队列，而如果toFlush
                    // 不为空，则调用bufferQueue.poll，这是提高性能的一种做法。
                    if (toFlush.isEmpty()) {
                        req = this.bufferQueue.take();
                    }
                    else {
                        req = this.bufferQueue.poll();
                        // 如果当前请求为空，表明等待写入的消息已经没有了，这时候文件缓存中的消息需要提交到磁盘，防止消息丢失；
                        // 或者如果已经写入文件的大小大于maxTransferSize，则提交到磁盘
                        // 这里需要注意的是，会出现这样一种情况，刚好最后一个segment的文件快满了，这时候是不会roll出一个新的segment写
                        // 入消息的，而是直接追加到原来的segment尾部，可能导致segment对应的文件大小大于配置的单个segment大小
                        if (req == null || last.fileMessageSet.getSizeInBytes() > lastFlushPos + this.maxTransferSize) {
                            // 强制force，确保内容保存到磁盘
                            last.fileMessageSet.flush();
                            lastFlushPos = last.fileMessageSet.highWaterMark();
                            // 通知回调
                            // 异步写入比组写入可靠，因为异步写入一定是提交到磁盘的时候才进行回调的，而组写入如果依赖组提交的方式，则可
                            // 能会丢失数据，因为组写入在消息写入到文件缓存的时候就进行回调了(除非设置unflushThreshold=1)
                            for (final WriteRequest request : toFlush) {
                                request.cb.appendComplete(request.result);
                            }
                            toFlush.clear();
                            // 是否需要roll
                            this.mayBeRoll();
                            // 如果切换文件，重新获取last
                            if (this.segments.last() != last) {
                                last = null;
                            }
                            continue;
                        }
                    }
                }

                if (req == null) {
                    continue;
                }
                // 写入文件，并计算写入位置
                final int remainning = req.buf.remaining();
                // 写入位置为：当前segment给定的值 + 加上文件已有的长度
                final long offset = last.start + last.fileMessageSet.append(req.buf);
                req.result = Location.create(offset, remainning);
                if (req.cb != null) {
                    toFlush.add(req);
                }
                req = null;
            }
            catch (final IOException e) {
                log.error("Append message failed,*critical error*,the group commit thread would be terminated.", e);
                // TODO io异常没办法处理了，简单跳出?
                break;
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }

        // terminated
        // 关闭store 前，将等待写入队列中的剩余消息写入最后一个文件，这时候如果最后一个Segment满了也不会roll出新的Segment，持续的将消息
        // 写入到最后一个Segment，所以这时候也会发生Segment的size大于配置的size的情况
        try {
            for (WriteRequest request : this.bufferQueue) {
                final int remainning = request.buf.remaining();
                final long offset = last.start + last.fileMessageSet.append(request.buf);
                if (request.cb != null) {
                    request.cb.appendComplete(Location.create(offset, remainning));
                }
            }
            this.bufferQueue.clear();
        }
        catch (IOException e) {
            log.error("Append message failed", e);
        }

    }



    public String getDescription() {
        if (this.desc == null) {
            this.desc = this.topic + "-" + this.partition;
        }
        return this.desc;
    }

    public long getMessageCount() {
        long sum = 0;
        for (final Segment seg : this.segments.view()) {
            sum += seg.fileMessageSet.getMessageCount();
        }
        return sum;
    }

    public long getSizeInBytes() {
        long sum = 0;
        for (final Segment seg : this.segments.view()) {
            sum += seg.fileMessageSet.getSizeInBytes();
        }
        return sum;
    }

    SegmentList getSegments() {
        return this.segments;
    }

    File getPartitionDir() {
        return this.partitionDir;
    }

    /**
     * 关闭写入
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.closed = true;
        this.interrupt();
        // 等待子线程完成写完异步队列中剩余未写的消息
        try {
            this.join(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 关闭segment，保证内容都已经提交到磁盘
        for (final Segment segment : this.segments.view()) {
            segment.fileMessageSet.close();
        }
    }

    /**
     * 执行删除策略的任务
     */
    public void runDeletePolicy() {
        if (this.deletePolicy == null) {
            return;
        }
        final long start = System.currentTimeMillis();
        final Segment[] view = this.segments.view();
        for (final Segment segment : view) {
            // 非可变并且可删除
            if (!segment.fileMessageSet.isMutable() && this.deletePolicy.canDelete(segment.file, start)) {
                log.info("Deleting file " + segment.file.getAbsolutePath() + " with policy " + this.deletePolicy.name());
                this.segments.delete(segment);
                try {
                    segment.fileMessageSet.close();
                    this.deletePolicy.process(segment.file);
                }
                catch (final IOException e) {
                    log.error("关闭并删除file message set失败", e);
                }

            }
        }

    }

    /**
     * 从存储消息的分区目录中加载消息，offsetIfCreate 表示加载消息时，文件的起始偏移量
     * @param offsetIfCreate 表示消息的偏移量
     * @throws IOException
     */
    private void loadSegments(final long offsetIfCreate) throws IOException {
        // 表示该分区下的所有消息
        final List<Segment> accum = new ArrayList<Segment>();

        // 1、获取该分区下的消息文件列表（.meta文件），并加载到accum列表
        final File[] ls = this.partitionDir.listFiles();
        if (ls != null) {
            // 遍历分区目录下的所有.meta后缀的数据文件，将所有文件都变为不可变的文件
            for (final File file : ls) {
                // 判断是否为存储消息的文件，存储消息的文件都是.meta文件
                if (file.isFile() && file.toString().endsWith(FILE_SUFFIX)) {
                    if (!file.canRead()) {
                        throw new IOException("Could not read file " + file);
                    }
                    final String filename = file.getName();
                    // 获取消息文件的文件名，然后转为Long类型
                    final long start = Long.parseLong(filename.substring(0, filename.length() - FILE_SUFFIX.length()));
                    // 先作为不可变的加载进来
                    accum.add(new Segment(start, file, false));
                }
            }
        }

        // 2、对accum列表进行校验和排序
        if (accum.size() == 0) {
            // 没有可用的文件，创建一个，索引从offsetIfCreate开始
            final File newFile = new File(this.partitionDir, this.nameFromOffset(offsetIfCreate));
            accum.add(new Segment(offsetIfCreate, newFile));
        }
        else {
            // 至少有一个文件，校验并按照start升序排序
            Collections.sort(accum, new Comparator<Segment>() {
                @Override
                public int compare(final Segment o1, final Segment o2) {
                    if (o1.start == o2.start) {
                        return 0;
                    }
                    else if (o1.start > o2.start) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                }
            });
            // 校验分区目录下的消息文件，保证消息顺序存储
            this.validateSegments(accum);
            // 最后一个文件修改为可变
            final Segment last = accum.remove(accum.size() - 1);
            // 关闭消息文件的channel
            last.fileMessageSet.close();
            log.info("Loading the last segment in mutable mode and running recover on " + last.file.getAbsolutePath());
            final Segment mutable = new Segment(last.start, last.file);
            accum.add(mutable);
            log.info("Loaded " + accum.size() + " segments...");
        }

        // 3、校验完后封装为一个 SegmentList 对象并赋值
        // 多个segmentg通过SegmentList组织起来，SegmentList能保证在并发访问下的删除、添加保持一致性，SegmentList没有采用java的关键字
        // synchronized进行同步，而是使用类似cvs原语的方式进行同步访问（因为绝大部分情况下并没有并发问题，可以极大的提高效率）
        this.segments = new SegmentList(accum.toArray(new Segment[accum.size()]));
    }

    /**
     * 校验分区目录下的消息文件，保证消息顺序存储
     * @param segments
     */
    private void validateSegments(final List<Segment> segments) {
        // 验证按升序排序的Segment是否前后衔接，确保文件没有被篡改和破坏(这里的验证是比较简单的验证，消息内容的验证在FileMessageSet中，
        // 通过比较checksum进行验证，在前面的篇幅中介绍过，这两种方式结合可以在范围上从大到小进行验证，保证内容基本不被破坏和篡改)
        this.writeLock.lock();
        try {
            for (int i = 0; i < segments.size() - 1; i++) {
                final Segment curr = segments.get(i);
                final Segment next = segments.get(i + 1);
                if (curr.start + curr.size() != next.start) {
                    throw new IllegalStateException("The following segments don't validate: "
                            + curr.file.getAbsolutePath() + ", " + next.file.getAbsolutePath());
                }
            }
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 检查该目录是否为文件夹
     * @param dir
     */
    private void checkDir(final File dir) {
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                throw new RuntimeException("Create directory failed:" + dir.getAbsolutePath());
            }
        }
        if (!dir.isDirectory()) {
            throw new RuntimeException("Path is not a directory:" + dir.getAbsolutePath());
        }
    }




    // 将消息保存到消息存储器

    /**
     * 将消息保存到消息存储管理器，当客户端put消息到MQ服务器时，会调用该方法，将消息存储到消息存储器
     *
     * @param msgId 消息id
     * @param req   put请求
     * @param cb    回调接口
     */
    public void append(final long msgId, final PutCommand req, final AppendCallback cb) {
        this.appendBuffer(MessageUtils.makeMessageBuffer(msgId, req), cb);
    }
    /**
     * Append多个消息，返回写入的位置
     *
     * @param msgIds
     * @param putCmds
     * @param cb
     *
     * @return
     */
    public void append(final List<Long> msgIds, final List<PutCommand> putCmds, final AppendCallback cb) {
        this.appendBuffer(MessageUtils.makeMessageBuffer(msgIds, putCmds), cb);
    }
    /**
     * 这里比较好的设计是采用回调的方式来，对于异步写入实现就变得非常容易，AppendCallback返回的是消息成功写入的位置Location(起始位置和消
     * 息长度)，该Location并不是相对于当前Segment的开始位置0，而是相对于当前Segment给定的值(对应文件命名值即为给定的值)，以后查询消息的
     * 时候直接使用该位置就可以快速定位到消息写入到哪个文件，这也就是为什么文件名的命名采用前后衔接的方式，这也通过2分查找可以快速定位消息的位置
     *
     * @param buffer    表示一个消息文件的数据
     * @param cb        回调接口
     */
    private void appendBuffer(final ByteBuffer buffer, final AppendCallback cb) {
        if (this.closed) {
            throw new IllegalStateException("Closed MessageStore.");
        }

        // 如果启动异步写入并且消息长度小于一次提交的最大值maxTransferSize，则将该消息放入异步写入队列
        if (this.useGroupCommit() && buffer.remaining() < this.maxTransferSize) {
            this.bufferQueue.offer(new WriteRequest(buffer, cb));
        }
        else {
            Location location = null;
            final int remainning = buffer.remaining();
            this.writeLock.lock();
            try {
                final Segment cur = this.segments.last();
                // 将消息文件数据追加到fileMessageSet中，并返回追加前的fileMessageSet大小
                final long offset = cur.start + cur.fileMessageSet.append(buffer);
                // 根据消息的数量，判断是否要立即写入磁盘，当达到满足写入磁盘条件时，会立即写入
                this.mayBeFlush(1);
                // 判断segments的可变segment是否要指向下一个segment
                this.mayBeRoll();
                // offset:表示消息存入消息存储器（MessageStore）的位置；remainning:保存的消息数据的大小
                location = Location.create(offset, remainning);
            }
            catch (final IOException e) {
                log.error("Append file failed", e);
                location = Location.InvalidLocaltion;
            }
            finally {
                this.writeLock.unlock();
                if (cb != null) {
                    // 调用回调方法，数据写入文件缓存
                    cb.appendComplete(location);
                }
            }
        }
    }

    private void notifyCallback(AppendCallback callback, Location location) {
        try {
            callback.appendComplete(location);
        }
        catch (Exception e) {
            log.error("Call AppendCallback failed", e);
        }
    }

    /**
     * 判断是否启用异步写入：
     * 1、如果设置为unflushThreshold <=0的数字，则认为启动异步写入；
     * 2、如果设置为unflushThreshold =1，则是同步写入，即每写入一个消息都会提交到磁盘；
     * 3、如果unflushThreshold>0，则是依赖组提交或者是超时提交
     * @return
     */
    private boolean useGroupCommit() {
        return this.unflushThreshold <= 0;
    }





    /**
     * 重放事务操作，如果消息没有存储成功，则重新存储，并返回新的位置
     * @param offset
     * @param length
     * @param checksum
     * @param msgIds
     * @param reqs
     * @param cb
     * @throws IOException
     */
    public void replayAppend(final long offset, final int length, final int checksum, final List<Long> msgIds, final List<PutCommand> reqs, final AppendCallback cb) throws IOException {
        // 如果消息没有存储，则重新存储，写到最后一个Segment尾部
        final Segment segment = this.findSegment(this.segments.view(), offset);
        if (segment == null) {
            this.append(msgIds, reqs, cb);
        }
        else {
            final MessageSet messageSet =
                    segment.fileMessageSet.slice(offset - segment.start, offset - segment.start + length);
            final ByteBuffer buf = ByteBuffer.allocate(length);
            messageSet.read(buf, offset - segment.start);
            buf.flip();
            final byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            // 这个校验和是整个消息的校验和，这跟message的校验和不一样，注意区分
            final int checkSumInDisk = CheckSum.crc32(bytes);
            // 没有存入，则重新存储
            if (checksum != checkSumInDisk) {
                this.append(msgIds, reqs, cb);
            }
            else {
                // 正常存储了消息，无需处理
                if (cb != null) {
                    this.notifyCallback(cb, null);
                }
            }
        }
    }

    public String getTopic() {
        return this.topic;
    }

    public int getPartition() {
        return this.partition;
    }

    /**
     * 判断是否需要roll，如果当前 messagestore最后一个segment的size >= 配置的segment size，则产生新的segment，并将新的segment作为最
     * 后一个segment，原来最后的segment提交一次，并将mutable设置为false
     * @throws IOException
     */
    private void mayBeRoll() throws IOException {
        if (this.segments.last().fileMessageSet.getSizeInBytes() >= this.metaConfig.getMaxSegmentSize()) {
            this.roll();
        }
    }

    /**
     * 根据消息的偏移量命名保存在本地消息的文件名
     * @param offset
     * @return
     */
    String nameFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + FILE_SUFFIX;
    }

    private void roll() throws IOException {
        final long newOffset = this.nextAppendOffset();
        final File newFile = new File(this.partitionDir, this.nameFromOffset(newOffset));
        this.segments.last().fileMessageSet.flush();
        this.segments.last().fileMessageSet.setMutable(false);
        this.segments.append(new Segment(newOffset, newFile));
    }

    /**
     * 返回下一个消息文件的offset，就是消息文件名的
     *
     * @return
     * @throws IOException
     */
    private long nextAppendOffset() throws IOException {
        final Segment last = this.segments.last();
        last.fileMessageSet.flush();
        // 下一个消息文件名 = 当前最后一个消息文件名的 + 当前最后一个消息文件的大小
        return last.start + last.size();
    }

    /**
     * 根据添加的消息数量再决定是否要立即写入磁盘
     *
     * @param numOfMessages
     * @throws IOException
     */
    private void mayBeFlush(final int numOfMessages) throws IOException {
        // 如果这个topic的消息的最后写入磁盘的时间>配置时间 或者 这个topic还没写入磁盘的消息数量>配置的数量，则立即写入磁盘
        if (this.unflushed.addAndGet(numOfMessages) > this.metaConfig.getTopicConfig(this.topic).getUnflushThreshold()
                || SystemTimer.currentTimeMillis() - this.lastFlushTime.get() > this.metaConfig.getTopicConfig(
                    this.topic).getUnflushInterval()) {
            this.flush0();
        }
    }

    /**
     * 返回segment的信息，主要包括segment的开始位置以及 segment 的size
     * @return
     */
    public List<SegmentInfo> getSegmentInfos() {
        final List<SegmentInfo> rt = new ArrayList<SegmentInfo>();
        for (final Segment seg : this.segments.view()) {
            rt.add(new SegmentInfo(seg.start, seg.size()));
        }
        return rt;
    }

    /**
     * 将消息管理器中的消息flush到磁盘
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        this.writeLock.lock();
        try {
            this.flush0();
        }
        finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 将消息管理器中的消息flush到磁盘
     *
     * @throws IOException
     */
    private void flush0() throws IOException {
        // 如果是使用异步的批量提交，则停止flush
        if (this.useGroupCommit()) {
            return;
        }

        // 由于只有最后一个segment是可变，即可写入消息的，所以只需要提交最后一个segment的消息
        this.segments.last().fileMessageSet.flush();
        this.unflushed.set(0);
        this.lastFlushTime.set(SystemTimer.currentTimeMillis());
    }

    /**
     * 返回当前最大可读的offset
     * 需要注意的是，在文件缓存中的消息是不可读的，可以通过getSizeInBytes（）方法来判断还有多少内容还在文件缓存中，getSizeInBytes()方法
     * 返回的值是包括所有在磁盘和缓存中的size
     * 
     * @return
     */
    public long getMaxOffset() {
        final Segment last = this.segments.last();
        if (last != null) {
            return last.start + last.size();
        }
        else {
            return 0;
        }
    }

    /**
     * 返回当前最小可读的offset
     * 
     * @return
     */
    public long getMinOffset() {
        Segment first = this.segments.first();
        if (first != null) {
            return first.start;
        }
        else {
            return 0;
        }
    }

    /**
     * 根据offset和maxSize返回所在MessageSet, 当offset超过最大offset的时候返回null，
     * 当offset小于最小offset的时候抛出ArrayIndexOutOfBounds异常
     * 
     * @param offset
     * 
     * @param maxSize
     * @return
     * @throws IOException
     */
    public MessageSet slice(final long offset, final int maxSize) throws IOException {
        final Segment segment = this.findSegment(this.segments.view(), offset);
        if (segment == null) {
            return null;
        }
        else {
            return segment.fileMessageSet.slice(offset - segment.start, offset - segment.start + maxSize);
        }
    }

    /**
     * 返回离指定offset往前追溯最近的可用offset ,当传入的offset超出范围的时候返回边界offset
     * 
     * @param offset
     * @return
     */
    public long getNearestOffset(final long offset) {
        return this.getNearestOffset(offset, this.segments);
    }

    long getNearestOffset(final long offset, final SegmentList segments) {
        try {
            final Segment segment = this.findSegment(segments.view(), offset);
            if (segment != null) {
                return segment.start;
            }
            else {
                final Segment last = segments.last();
                return last.start + last.size();
            }
        }
        catch (final ArrayIndexOutOfBoundsException e) {
            return segments.first().start;
        }
    }

    /**
     * 根据offset查找文件,如果超过尾部，则返回null，如果在头部之前，则抛出ArrayIndexOutOfBoundsException
     * 指定位置找到对应的segment，由于前面的文件组织方式，所以这里可以采用2分查找的方式，效率很高
     * @param segments
     * @param offset
     * @return 返回找到segment，如果超过尾部，则返回null，如果在头部之前，则抛出异常
     * @throws ArrayIndexOutOfBoundsException
     */
    Segment findSegment(final Segment[] segments, final long offset) {
        if (segments == null || segments.length < 1) {
            return null;
        }
        // 老的数据不存在，返回最近最老的数据
        final Segment last = segments[segments.length - 1];
        // 在头部以前，抛出异常
        if (offset < segments[0].start) {
            throw new ArrayIndexOutOfBoundsException();
        }
        // 刚好在尾部或者超出范围，返回null
        if (offset >= last.start + last.size()) {
            return null;
        }
        // 根据offset二分查找
        int low = 0;
        int high = segments.length - 1;
        while (low <= high) {
            final int mid = high + low >>> 1;
        final Segment found = segments[mid];
        if (found.contains(offset)) {
            return found;
        }
        else if (offset < found.start) {
            high = mid - 1;
        }
        else {
            low = mid + 1;
        }
        }
        return null;
    }


    /**
     * 消息文件异步写入内存的包装类
     */
    private static class WriteRequest {
        public final ByteBuffer buf;
        public final AppendCallback cb;
        public Location result;

        public WriteRequest(final ByteBuffer buf, final AppendCallback cb) {
            super();
            this.buf = buf;
            this.cb = cb;
        }
    }
}