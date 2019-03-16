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
 * {@link MessageStore} ʵ������һ��������ʵ����һ��topic����Ϣ�洢���ڲ��������ļ�(segment)
 * {@link MessageStore} �̳���Thread�࣬�̳и�����Ҫ��Ϊ��ʵ���첽д�뷽ʽ
 *
 * ��Ϣ��broker�ϵ�ÿ������������֯��һ���ļ��б���������ȡ������Ҫ֪���������ļ��е�ƫ���������ƫ����������νoffset��
 * Offset�Ǿ���ƫ�������������Ὣoffsetת��Ϊ�����ļ������ƫ����
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
     * ��ʾһ����Ϣ�ļ�
     *
     * MessageStore����Segment��ʽ��֯�洢��Segment��װ��FileMessageSet����FileMessageSet���ж�д��MessageStore�������Segment
     * ����ǰ���νӣ��νӷ�ʽΪ��
     * ��һ��Segment��Ӧ����Ϣ�ļ�����Ϊ0.meta;
     * �ڶ���������Ϊ��һ���ļ��Ŀ�ʼλ��+��һ��Segment�Ĵ�С;
     * ͼʾ����(��������ÿ���ļ���С��Ϊ1024byte)��
     * 0.meta -> 1024.meta -> 2048.meta -> ...
     *
     * ΪʲôҪ������������أ���Ҫ��Ϊ����߲�ѯЧ�ʡ�MessageStore�����һ��Segment��Ϊ�ɱ�Segment����Ϊ���һ��Segment�൱���ļ�β��
     * ��Ϣ�����Ⱥ�˳��ģ����뽫��Ϣ��ӵ����һ��Segment�ϡ�
     */
    static class Segment {
        /** ��Ƭ�δ����offset��Ҳ���������Ϣ�ļ����ļ���������.mate��׺������������Ϊ��Ϣ��id������ʵ���ϲ�������Ϣ��id��*/
        final long start;
        /** ��ʾ���浽���̵���Ϣ�ļ���һ����Ϣ��Ӧһ���ļ� */
        final File file;
        /** ��Ƭ�ε���Ϣ���� */
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
                // // ���ɱ�ģ����ﲻ��ֱ����FileMessageSet(channel, false)
                // if (mutable == true) {
                // this.fileMessageSet.setMutable(true);
                // }
            }
            catch (final IOException e) {
                log.error("��ʼ����Ϣ����ʧ��", e);
            }
        }

        /** ��ȡƬ�δ�С */
        public long size() {
            return this.fileMessageSet.highWaterMark();
        }

        /** �ж�offset�Ƿ��ڱ��ļ��� */
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
     * ���ɱ��segment list
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
         * �����Segment׷�ӵ�SegmentList
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
         * ��SegmentListɾ��ָ����SegmentԪ��
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
                // ����ǰ���
                System.arraycopy(curr, 0, update, 0, index);
                // ��������
                if (index + 1 < curr.length) {
                    System.arraycopy(curr, index + 1, update, index, curr.length - index - 1);
                }
                if (this.contents.compareAndSet(curr, update)) {
                    return;
                }
            }
        }

        /**
         * ��ȡSegmentList�е�Ԫ������
         *
         * @return
         */
        public Segment[] view() {
            return this.contents.get();
        }

        /**
         * ��ȡ���һ��
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
         * ��ȡ��һ��
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
    /** ��ʾ�洢��Ϣ�ļ��ĺ�׺ */
    private static final String FILE_SUFFIX = ".meta";
    /** ��ʾ�����Ƿ񱻹ر� */
    private volatile boolean closed = false;
    /** �ӷ���Ŀ¼�м�����Ϣ�󣬻Ὣ��Ϣ���浽���б��У���ʼ����Ϣ�洢��ʱ���Ὣ���̵���Ϣ�ļ�Ҳ���ؽ����� */
    private SegmentList segments;
    /** ��ʾ����Ϣ���ڱ��ش��̵ķ���Ŀ¼������Ŀ¼���кܶ�.meta����Ϣ�ļ� */
    private final File partitionDir;
    /** ��ʾ��Ϣ������topic */
    private final String topic;
    /** ��ʾ��Ϣ�����ķ������� */
    private final int partition;
    /** ȫ��MQ���� */
    private final MetaConfig metaConfig;
    /** �����Ƕ��ļ��Ĵ洢��ʽ�����ѹ�����Ϣ�������Ϣ��Ҫɾ���Ӷ��ڳ��ռ������Ϣ�ģ�Ĭ���ṩ�鵵�͹���ɾ���ķ�ʽ  */
    private final DeletePolicy deletePolicy;

    private final AtomicInteger unflushed;
    /** ��ʾ���һ��flush��Ϣ�����̵�ʱ�� */
    private final AtomicLong lastFlushTime;

    /** ����������ߵ�������ݴ�С,Ĭ��Ϊ1M */
    private long maxTransferSize;
    /** ����������첽д�룬�������첽д���̣߳����unflushThreshold<= 0������Ϊ�����첽д��ķ�ʽ�� */
    int unflushThreshold = 1000;

    /** ��ʾ�첽��Ϣ����ʱ��д����У����첽�ķ�ʽ�ӷ���Ŀ¼�ж�ȡ��Ϣ�ļ������ͻ������첽�ķ�ʽ����Ϣʱ���Ὣ��Ϣ�ȱ����ڸö��У����������첽�ķ�ʽд����� */
    private final LinkedTransferQueue<WriteRequest> bufferQueue = new LinkedTransferQueue<WriteRequest>();

    private volatile String desc;

    /** У�����Ŀ¼�µ���Ϣ�ļ�ʱ��������м��� */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     *
     * @param topic
     * @param partition
     * @param metaConfig
     * @param deletePolicy
     * @param offsetIfCreate ��ʾ��ǰ��������Ϣ��ƫ����
     * @throws IOException
     */
    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig, final DeletePolicy deletePolicy, final long offsetIfCreate) throws IOException {
        this.metaConfig = metaConfig;
        this.topic = topic;
        final TopicConfig topicConfig = this.metaConfig.getTopicConfig(this.topic);

        // 1����ȡ��Ϣ�洢�ڴ��̵ĸ�Ŀ¼
        String dataPath = metaConfig.getDataPath();
        if (topicConfig != null) {
            dataPath = topicConfig.getDataPath();
        }
        final File parentDir = new File(dataPath);
        this.checkDir(parentDir);

        // 2����ȡ��Ϣ���ڵķ���Ŀ¼
        this.partitionDir = new File(dataPath + File.separator + topic + "-" + partition);
        this.checkDir(this.partitionDir);

        this.partition = partition;
        this.unflushed = new AtomicInteger(0);
        this.lastFlushTime = new AtomicLong(SystemTimer.currentTimeMillis());
        this.unflushThreshold = topicConfig.getUnflushThreshold();
        this.deletePolicy = deletePolicy;

        // Make a copy to avoid getting it again and again.
        this.maxTransferSize = metaConfig.getMaxTransferSize();
        // �����첽д���ʱ����Ϣ�ύ�����̵�size���ã�ͬʱҲ��������д��ʱ����Ϣ��󳤶ȵĿ��Ʋ����������Ϣ���ȴ��ڸò��������ͬ��д��
        this.maxTransferSize = this.maxTransferSize > ONE_M_BYTES ? ONE_M_BYTES : this.maxTransferSize;

        // Check directory and load exists segments.
        this.checkDir(this.partitionDir);
        this.loadSegments(offsetIfCreate);

        // �Ƿ������첽�ķ�ʽ������Ϣд�����
        if (this.useGroupCommit()) {
            this.start();
        }
    }
    public MessageStore(final String topic, final int partition, final MetaConfig metaConfig, final DeletePolicy deletePolicy) throws IOException {
        this(topic, partition, metaConfig, deletePolicy, 0);
    }


    /**
     * ���̷߳������ڽ���Ϣ�洢�������е���Ϣ���浽����
     */
    @Override
    public void run() {
        // �ȴ�force�Ķ���
        final LinkedList<WriteRequest> toFlush = new LinkedList<WriteRequest>();
        WriteRequest req = null;
        long lastFlushPos = 0;
        Segment last = null;

        // �洢û�йرղ����߳�û�б��ж�
        while (!this.closed && !Thread.currentThread().isInterrupted()) {
            try {

                if (last == null) {
                    // ��ȡ����һ��segment������Ϣд�����segment��Ӧ���ļ�
                    last = this.segments.last();
                    lastFlushPos = last.fileMessageSet.highWaterMark();
                }

                if (req == null) {
                    // ����ȴ��ύ�����̵Ķ���toFlushΪ�գ������ֿ��ܣ�
                    // һ���ո��ύ�꣬�б�Ϊ�գ�
                    // �����ȴ�д����Ϣ�Ķ���Ϊ�գ�����ж�toFlushΪ�գ������bufferQueue.take()��������������ס���У������toFlush
                    // ��Ϊ�գ������bufferQueue.poll������������ܵ�һ��������
                    if (toFlush.isEmpty()) {
                        req = this.bufferQueue.take();
                    }
                    else {
                        req = this.bufferQueue.poll();
                        // �����ǰ����Ϊ�գ������ȴ�д�����Ϣ�Ѿ�û���ˣ���ʱ���ļ������е���Ϣ��Ҫ�ύ�����̣���ֹ��Ϣ��ʧ��
                        // ��������Ѿ�д���ļ��Ĵ�С����maxTransferSize�����ύ������
                        // ������Ҫע����ǣ����������һ��������պ����һ��segment���ļ������ˣ���ʱ���ǲ���roll��һ���µ�segmentд
                        // ����Ϣ�ģ�����ֱ��׷�ӵ�ԭ����segmentβ�������ܵ���segment��Ӧ���ļ���С�������õĵ���segment��С
                        if (req == null || last.fileMessageSet.getSizeInBytes() > lastFlushPos + this.maxTransferSize) {
                            // ǿ��force��ȷ�����ݱ��浽����
                            last.fileMessageSet.flush();
                            lastFlushPos = last.fileMessageSet.highWaterMark();
                            // ֪ͨ�ص�
                            // �첽д�����д��ɿ�����Ϊ�첽д��һ�����ύ�����̵�ʱ��Ž��лص��ģ�����д������������ύ�ķ�ʽ�����
                            // �ܻᶪʧ���ݣ���Ϊ��д������Ϣд�뵽�ļ������ʱ��ͽ��лص���(��������unflushThreshold=1)
                            for (final WriteRequest request : toFlush) {
                                request.cb.appendComplete(request.result);
                            }
                            toFlush.clear();
                            // �Ƿ���Ҫroll
                            this.mayBeRoll();
                            // ����л��ļ������»�ȡlast
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
                // д���ļ���������д��λ��
                final int remainning = req.buf.remaining();
                // д��λ��Ϊ����ǰsegment������ֵ + �����ļ����еĳ���
                final long offset = last.start + last.fileMessageSet.append(req.buf);
                req.result = Location.create(offset, remainning);
                if (req.cb != null) {
                    toFlush.add(req);
                }
                req = null;
            }
            catch (final IOException e) {
                log.error("Append message failed,*critical error*,the group commit thread would be terminated.", e);
                // TODO io�쳣û�취�����ˣ�������?
                break;
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }

        // terminated
        // �ر�store ǰ�����ȴ�д������е�ʣ����Ϣд�����һ���ļ�����ʱ��������һ��Segment����Ҳ����roll���µ�Segment�������Ľ���Ϣ
        // д�뵽���һ��Segment��������ʱ��Ҳ�ᷢ��Segment��size�������õ�size�����
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
     * �ر�д��
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        this.closed = true;
        this.interrupt();
        // �ȴ����߳����д���첽������ʣ��δд����Ϣ
        try {
            this.join(500);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // �ر�segment����֤���ݶ��Ѿ��ύ������
        for (final Segment segment : this.segments.view()) {
            segment.fileMessageSet.close();
        }
    }

    /**
     * ִ��ɾ�����Ե�����
     */
    public void runDeletePolicy() {
        if (this.deletePolicy == null) {
            return;
        }
        final long start = System.currentTimeMillis();
        final Segment[] view = this.segments.view();
        for (final Segment segment : view) {
            // �ǿɱ䲢�ҿ�ɾ��
            if (!segment.fileMessageSet.isMutable() && this.deletePolicy.canDelete(segment.file, start)) {
                log.info("Deleting file " + segment.file.getAbsolutePath() + " with policy " + this.deletePolicy.name());
                this.segments.delete(segment);
                try {
                    segment.fileMessageSet.close();
                    this.deletePolicy.process(segment.file);
                }
                catch (final IOException e) {
                    log.error("�رղ�ɾ��file message setʧ��", e);
                }

            }
        }

    }

    /**
     * �Ӵ洢��Ϣ�ķ���Ŀ¼�м�����Ϣ��offsetIfCreate ��ʾ������Ϣʱ���ļ�����ʼƫ����
     * @param offsetIfCreate ��ʾ��Ϣ��ƫ����
     * @throws IOException
     */
    private void loadSegments(final long offsetIfCreate) throws IOException {
        // ��ʾ�÷����µ�������Ϣ
        final List<Segment> accum = new ArrayList<Segment>();

        // 1����ȡ�÷����µ���Ϣ�ļ��б�.meta�ļ����������ص�accum�б�
        final File[] ls = this.partitionDir.listFiles();
        if (ls != null) {
            // ��������Ŀ¼�µ�����.meta��׺�������ļ����������ļ�����Ϊ���ɱ���ļ�
            for (final File file : ls) {
                // �ж��Ƿ�Ϊ�洢��Ϣ���ļ����洢��Ϣ���ļ�����.meta�ļ�
                if (file.isFile() && file.toString().endsWith(FILE_SUFFIX)) {
                    if (!file.canRead()) {
                        throw new IOException("Could not read file " + file);
                    }
                    final String filename = file.getName();
                    // ��ȡ��Ϣ�ļ����ļ�����Ȼ��תΪLong����
                    final long start = Long.parseLong(filename.substring(0, filename.length() - FILE_SUFFIX.length()));
                    // ����Ϊ���ɱ�ļ��ؽ���
                    accum.add(new Segment(start, file, false));
                }
            }
        }

        // 2����accum�б����У�������
        if (accum.size() == 0) {
            // û�п��õ��ļ�������һ����������offsetIfCreate��ʼ
            final File newFile = new File(this.partitionDir, this.nameFromOffset(offsetIfCreate));
            accum.add(new Segment(offsetIfCreate, newFile));
        }
        else {
            // ������һ���ļ���У�鲢����start��������
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
            // У�����Ŀ¼�µ���Ϣ�ļ�����֤��Ϣ˳��洢
            this.validateSegments(accum);
            // ���һ���ļ��޸�Ϊ�ɱ�
            final Segment last = accum.remove(accum.size() - 1);
            // �ر���Ϣ�ļ���channel
            last.fileMessageSet.close();
            log.info("Loading the last segment in mutable mode and running recover on " + last.file.getAbsolutePath());
            final Segment mutable = new Segment(last.start, last.file);
            accum.add(mutable);
            log.info("Loaded " + accum.size() + " segments...");
        }

        // 3��У������װΪһ�� SegmentList ���󲢸�ֵ
        // ���segmentgͨ��SegmentList��֯������SegmentList�ܱ�֤�ڲ��������µ�ɾ������ӱ���һ���ԣ�SegmentListû�в���java�Ĺؼ���
        // synchronized����ͬ��������ʹ������cvsԭ��ķ�ʽ����ͬ�����ʣ���Ϊ���󲿷�����²�û�в������⣬���Լ�������Ч�ʣ�
        this.segments = new SegmentList(accum.toArray(new Segment[accum.size()]));
    }

    /**
     * У�����Ŀ¼�µ���Ϣ�ļ�����֤��Ϣ˳��洢
     * @param segments
     */
    private void validateSegments(final List<Segment> segments) {
        // ��֤�����������Segment�Ƿ�ǰ���νӣ�ȷ���ļ�û�б��۸ĺ��ƻ�(�������֤�ǱȽϼ򵥵���֤����Ϣ���ݵ���֤��FileMessageSet�У�
        // ͨ���Ƚ�checksum������֤����ǰ���ƪ���н��ܹ��������ַ�ʽ��Ͽ����ڷ�Χ�ϴӴ�С������֤����֤���ݻ��������ƻ��ʹ۸�)
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
     * ����Ŀ¼�Ƿ�Ϊ�ļ���
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




    // ����Ϣ���浽��Ϣ�洢��

    /**
     * ����Ϣ���浽��Ϣ�洢�����������ͻ���put��Ϣ��MQ������ʱ������ø÷���������Ϣ�洢����Ϣ�洢��
     *
     * @param msgId ��Ϣid
     * @param req   put����
     * @param cb    �ص��ӿ�
     */
    public void append(final long msgId, final PutCommand req, final AppendCallback cb) {
        this.appendBuffer(MessageUtils.makeMessageBuffer(msgId, req), cb);
    }
    /**
     * Append�����Ϣ������д���λ��
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
     * ����ȽϺõ�����ǲ��ûص��ķ�ʽ���������첽д��ʵ�־ͱ�÷ǳ����ף�AppendCallback���ص�����Ϣ�ɹ�д���λ��Location(��ʼλ�ú���
     * Ϣ����)����Location����������ڵ�ǰSegment�Ŀ�ʼλ��0����������ڵ�ǰSegment������ֵ(��Ӧ�ļ�����ֵ��Ϊ������ֵ)���Ժ��ѯ��Ϣ��
     * ʱ��ֱ��ʹ�ø�λ�þͿ��Կ��ٶ�λ����Ϣд�뵽�ĸ��ļ�����Ҳ����Ϊʲô�ļ�������������ǰ���νӵķ�ʽ����Ҳͨ��2�ֲ��ҿ��Կ��ٶ�λ��Ϣ��λ��
     *
     * @param buffer    ��ʾһ����Ϣ�ļ�������
     * @param cb        �ص��ӿ�
     */
    private void appendBuffer(final ByteBuffer buffer, final AppendCallback cb) {
        if (this.closed) {
            throw new IllegalStateException("Closed MessageStore.");
        }

        // ��������첽д�벢����Ϣ����С��һ���ύ�����ֵmaxTransferSize���򽫸���Ϣ�����첽д�����
        if (this.useGroupCommit() && buffer.remaining() < this.maxTransferSize) {
            this.bufferQueue.offer(new WriteRequest(buffer, cb));
        }
        else {
            Location location = null;
            final int remainning = buffer.remaining();
            this.writeLock.lock();
            try {
                final Segment cur = this.segments.last();
                // ����Ϣ�ļ�����׷�ӵ�fileMessageSet�У�������׷��ǰ��fileMessageSet��С
                final long offset = cur.start + cur.fileMessageSet.append(buffer);
                // ������Ϣ���������ж��Ƿ�Ҫ����д����̣����ﵽ����д���������ʱ��������д��
                this.mayBeFlush(1);
                // �ж�segments�Ŀɱ�segment�Ƿ�Ҫָ����һ��segment
                this.mayBeRoll();
                // offset:��ʾ��Ϣ������Ϣ�洢����MessageStore����λ�ã�remainning:�������Ϣ���ݵĴ�С
                location = Location.create(offset, remainning);
            }
            catch (final IOException e) {
                log.error("Append file failed", e);
                location = Location.InvalidLocaltion;
            }
            finally {
                this.writeLock.unlock();
                if (cb != null) {
                    // ���ûص�����������д���ļ�����
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
     * �ж��Ƿ������첽д�룺
     * 1���������ΪunflushThreshold <=0�����֣�����Ϊ�����첽д�룻
     * 2���������ΪunflushThreshold =1������ͬ��д�룬��ÿд��һ����Ϣ�����ύ�����̣�
     * 3�����unflushThreshold>0�������������ύ�����ǳ�ʱ�ύ
     * @return
     */
    private boolean useGroupCommit() {
        return this.unflushThreshold <= 0;
    }





    /**
     * �ط���������������Ϣû�д洢�ɹ��������´洢���������µ�λ��
     * @param offset
     * @param length
     * @param checksum
     * @param msgIds
     * @param reqs
     * @param cb
     * @throws IOException
     */
    public void replayAppend(final long offset, final int length, final int checksum, final List<Long> msgIds, final List<PutCommand> reqs, final AppendCallback cb) throws IOException {
        // �����Ϣû�д洢�������´洢��д�����һ��Segmentβ��
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
            // ���У�����������Ϣ��У��ͣ����message��У��Ͳ�һ����ע������
            final int checkSumInDisk = CheckSum.crc32(bytes);
            // û�д��룬�����´洢
            if (checksum != checkSumInDisk) {
                this.append(msgIds, reqs, cb);
            }
            else {
                // �����洢����Ϣ�����账��
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
     * �ж��Ƿ���Ҫroll�������ǰ messagestore���һ��segment��size >= ���õ�segment size��������µ�segment�������µ�segment��Ϊ��
     * ��һ��segment��ԭ������segment�ύһ�Σ�����mutable����Ϊfalse
     * @throws IOException
     */
    private void mayBeRoll() throws IOException {
        if (this.segments.last().fileMessageSet.getSizeInBytes() >= this.metaConfig.getMaxSegmentSize()) {
            this.roll();
        }
    }

    /**
     * ������Ϣ��ƫ�������������ڱ�����Ϣ���ļ���
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
     * ������һ����Ϣ�ļ���offset��������Ϣ�ļ�����
     *
     * @return
     * @throws IOException
     */
    private long nextAppendOffset() throws IOException {
        final Segment last = this.segments.last();
        last.fileMessageSet.flush();
        // ��һ����Ϣ�ļ��� = ��ǰ���һ����Ϣ�ļ����� + ��ǰ���һ����Ϣ�ļ��Ĵ�С
        return last.start + last.size();
    }

    /**
     * ������ӵ���Ϣ�����پ����Ƿ�Ҫ����д�����
     *
     * @param numOfMessages
     * @throws IOException
     */
    private void mayBeFlush(final int numOfMessages) throws IOException {
        // ������topic����Ϣ�����д����̵�ʱ��>����ʱ�� ���� ���topic��ûд����̵���Ϣ����>���õ�������������д�����
        if (this.unflushed.addAndGet(numOfMessages) > this.metaConfig.getTopicConfig(this.topic).getUnflushThreshold()
                || SystemTimer.currentTimeMillis() - this.lastFlushTime.get() > this.metaConfig.getTopicConfig(
                    this.topic).getUnflushInterval()) {
            this.flush0();
        }
    }

    /**
     * ����segment����Ϣ����Ҫ����segment�Ŀ�ʼλ���Լ� segment ��size
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
     * ����Ϣ�������е���Ϣflush������
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
     * ����Ϣ�������е���Ϣflush������
     *
     * @throws IOException
     */
    private void flush0() throws IOException {
        // �����ʹ���첽�������ύ����ֹͣflush
        if (this.useGroupCommit()) {
            return;
        }

        // ����ֻ�����һ��segment�ǿɱ䣬����д����Ϣ�ģ�����ֻ��Ҫ�ύ���һ��segment����Ϣ
        this.segments.last().fileMessageSet.flush();
        this.unflushed.set(0);
        this.lastFlushTime.set(SystemTimer.currentTimeMillis());
    }

    /**
     * ���ص�ǰ���ɶ���offset
     * ��Ҫע����ǣ����ļ������е���Ϣ�ǲ��ɶ��ģ�����ͨ��getSizeInBytes�����������жϻ��ж������ݻ����ļ������У�getSizeInBytes()����
     * ���ص�ֵ�ǰ��������ڴ��̺ͻ����е�size
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
     * ���ص�ǰ��С�ɶ���offset
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
     * ����offset��maxSize��������MessageSet, ��offset�������offset��ʱ�򷵻�null��
     * ��offsetС����Сoffset��ʱ���׳�ArrayIndexOutOfBounds�쳣
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
     * ������ָ��offset��ǰ׷������Ŀ���offset ,�������offset������Χ��ʱ�򷵻ر߽�offset
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
     * ����offset�����ļ�,�������β�����򷵻�null�������ͷ��֮ǰ�����׳�ArrayIndexOutOfBoundsException
     * ָ��λ���ҵ���Ӧ��segment������ǰ����ļ���֯��ʽ������������Բ���2�ֲ��ҵķ�ʽ��Ч�ʺܸ�
     * @param segments
     * @param offset
     * @return �����ҵ�segment���������β�����򷵻�null�������ͷ��֮ǰ�����׳��쳣
     * @throws ArrayIndexOutOfBoundsException
     */
    Segment findSegment(final Segment[] segments, final long offset) {
        if (segments == null || segments.length < 1) {
            return null;
        }
        // �ϵ����ݲ����ڣ�����������ϵ�����
        final Segment last = segments[segments.length - 1];
        // ��ͷ����ǰ���׳��쳣
        if (offset < segments[0].start) {
            throw new ArrayIndexOutOfBoundsException();
        }
        // �պ���β�����߳�����Χ������null
        if (offset >= last.start + last.size()) {
            return null;
        }
        // ����offset���ֲ���
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
     * ��Ϣ�ļ��첽д���ڴ�İ�װ��
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