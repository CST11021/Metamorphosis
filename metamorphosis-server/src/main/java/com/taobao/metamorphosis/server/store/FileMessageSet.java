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

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.util.RemotingUtils;
import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.utils.CheckSum;
import com.taobao.metamorphosis.utils.MessageUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 基于文件的消息集合
 *
 *      * MessageStore采用Segment方式组织存储，Segment包装了FileMessageSet，由FileMessageSet进行读写，MessageStore并将多个Segment
 *      * 进行前后衔接，衔接方式为：
 *      * 第一个Segment对应的消息文件命名为0.meta;
 *      * 第二个则命名为第一个文件的开始位置+第一个Segment的大小;
 *      * 图示如下(假设现在每个文件大小都为1024byte)：
 *      * 0.meta -> 1024.meta -> 2048.meta -> ...
 *      *
 *      * 为什么要这样进行设计呢，主要是为了提高查询效率。MessageStore将最后一个Segment变为可变Segment，因为最后一个Segment相当于文件尾，
 *      * 消息是有先后顺序的，必须将消息添加到最后一个Segment上。
 *
 * @author boyan
 * @Date 2011-4-21
 */
public class FileMessageSet implements MessageSet, Closeable {

    static final Log log = LogFactory.getLog(FileMessageSet.class);
    static final Log transferLog = LogFactory.getLog("TransferLog");

    private static boolean fastBoot = Boolean.valueOf(System.getProperty("meta.fast_boot", "false"));

    /** 消息保存在磁盘，一个消息保存为一个文件，该对象是连接文件的通道, 使用FileChannel, 您可以从文件中读取数据和将数据写入文件。 */
    private final FileChannel channel;
    /** 该消息集合中的消息数量 */
    private final AtomicLong messageCount;
    /** 表示该消息集合的数量大小 */
    private final AtomicLong sizeInBytes;
    /** 已经确保写入磁盘的水位 */
    private final AtomicLong highWaterMark;
    /** 镜像offset */
    private final long offset;
    /** 表示当前消息文件是否可变，分区下的最后一个消息文件是可变的 */
    private boolean mutable;

    public FileMessageSet(final FileChannel channel) throws IOException {
        this(channel, 0, 0, true);
    }
    public FileMessageSet(final FileChannel channel, final long offset, final long limit, final boolean mutable) throws IOException {
        super();
        this.channel = channel;
        this.offset = offset;
        this.messageCount = new AtomicLong(0);
        this.sizeInBytes = new AtomicLong(0);
        this.highWaterMark = new AtomicLong(0);
        this.mutable = mutable;

        if (mutable) {
            final long startMs = System.currentTimeMillis();
            final long truncated = this.recover();
            if (this.messageCount.get() > 0) {
                log.info("Recovery succeeded in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds. "
                        + truncated + " bytes truncated.");
            }
        } else {
            try {
                this.sizeInBytes.set(Math.min(channel.size(), limit) - offset);
                this.highWaterMark.set(this.sizeInBytes.get());
            } catch (final Exception e) {
                log.error("Set sizeInBytes error", e);
            }
        }
    }


    @Override
    public long getMessageCount() {
        return this.messageCount.get();
    }

    public long highWaterMark() {
        return this.highWaterMark.get();
    }

    @Override
    public long append(final ByteBuffer buf) throws IOException {
        if (!this.mutable) {
            throw new UnsupportedOperationException("Immutable message set");
        }
        final long offset = this.sizeInBytes.get();
        int sizeInBytes = 0;
        while (buf.hasRemaining()) {
            sizeInBytes += this.channel.write(buf);
        }
        //
        this.sizeInBytes.addAndGet(sizeInBytes);
        // 该消息集合中的消息数量+1
        this.messageCount.incrementAndGet();
        return offset;
    }

    @Override
    public void flush() throws IOException {
        // FileChannel.force()方法将通道里尚未写入磁盘的数据强制写到磁盘上。出于性能方面的考虑，操作系统会将数据缓存在内存中，所以无法
        // 保证写入到FileChannel里的数据一定会即时写到磁盘上。要保证这一点，需要调用force()方法。
        // force()方法有一个boolean类型的参数，指明是否同时将文件元数据（权限信息等）写到磁盘上。
        this.channel.force(true);
        this.highWaterMark.set(this.sizeInBytes.get());
    }

    /**
     * 返回一个MessageSet镜像，指定offset和长度
     */
    @Override
    public MessageSet slice(final long offset, final long limit) throws IOException {
        return new FileMessageSet(this.channel, offset, limit, false);
    }

    @Override
    public void read(final ByteBuffer bf, final long offset) throws IOException {
        int size = 0;
        while (bf.hasRemaining()) {
            final int l = this.channel.read(bf, offset + size);
            if (l < 0) {
                break;
            }
            size += l;
        }
    }

    @Override
    public void read(final ByteBuffer bf) throws IOException {
        this.read(bf, this.offset);
    }

    @Override
    public void write(final GetCommand getCommand, final SessionContext ctx) {
        final IoBuffer buf = this.makeHead(getCommand.getOpaque(), this.sizeInBytes.get());
        // transfer to socket
        this.tryToLogTransferInfo(getCommand, ctx.getConnection());
        ctx.getConnection().transferFrom(buf, null, this.channel, this.offset, this.sizeInBytes.get());
    }

    public long write(final WritableByteChannel socketChanel) throws IOException {
        try {
            return this.getFileChannel().transferTo(this.offset, this.getSizeInBytes(), socketChanel);
        } catch (final IOException e) {
            // Check to see if the IOException is being thrown due to
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5103988
            final String message = e.getMessage();
            if (message != null && message.contains("temporarily unavailable")) {
                return 0;
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        if (!this.channel.isOpen()) {
            return;
        }
        if (this.mutable) {
            // 将消息写到磁盘上
            this.flush();
        }
        this.channel.close();
    }

    /**
     * 记录日志
     *
     * @param getCommand
     * @param conn
     */
    private void tryToLogTransferInfo(final GetCommand getCommand, final Connection conn) {
        if (transferLog.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder("TransferLog[\r\n");
            sb.append("topic:").append(getCommand.getTopic()).append("\r\n");
            sb.append("group:").append(getCommand.getGroup()).append("\r\n");
            sb.append("partition:").append(getCommand.getPartition()).append("\r\n");
            sb.append("offset:").append(this.offset).append("\r\n");
            sb.append("sizeInBytes:").append(this.sizeInBytes.get()).append("\r\n");
            final String addrString =
                    conn != null ? RemotingUtils.getAddrString(conn.getRemoteSocketAddress()) : "unknown";
            sb.append("client:").append(addrString).append("\r\n");
            sb.append("]\r\n");
            transferLog.debug(sb.toString());
        }
    }

    // value totalLen opaque\r\n
    IoBuffer makeHead(final int opaque, final long size) {
        final IoBuffer buf = IoBuffer.allocate(9 + ByteUtils.stringSize(opaque) + ByteUtils.stringSize(size));
        ByteUtils.setArguments(buf, "value", size, opaque);
        buf.flip();
        return buf;
    }

    private long recover() throws IOException {
        if (fastBoot) {
            final long size = this.channel.size();
            this.sizeInBytes.set(size);
            this.highWaterMark.set(size);
            this.messageCount.set(0);
            this.channel.position(size);
            return 0;
        }
        if (!this.mutable) {
            throw new UnsupportedOperationException("Immutable message set");
        }
        final long len = this.channel.size();
        final ByteBuffer buf = ByteBuffer.allocate(MessageUtils.HEADER_LEN);
        long validUpTo = 0L;
        long next = 0L;
        long msgCount = 0;
        do {
            next = this.validateMessage(buf, validUpTo, len);
            if (next >= 0) {
                msgCount++;
                validUpTo = next;
            }
        } while (next >= 0);
        this.channel.truncate(validUpTo);
        this.sizeInBytes.set(validUpTo);
        this.highWaterMark.set(validUpTo);
        this.messageCount.set(msgCount);
        this.channel.position(validUpTo);
        return len - validUpTo;
    }

    /**
     * 校验消息md5是否正确
     *
     * @param buf
     * @param start
     * @param len
     * @return
     * @throws IOException
     */
    private long validateMessage(final ByteBuffer buf, final long start, final long len) throws IOException {
        buf.rewind();
        long read = this.channel.read(buf);
        if (read < MessageUtils.HEADER_LEN) {
            return -1;
        }
        buf.flip();
        final int messageLen = buf.getInt();
        final long next = start + MessageUtils.HEADER_LEN + messageLen;
        if (next > len) {
            return -1;
        }
        final int checksum = buf.getInt();
        if (messageLen < 0) {
            // 数据损坏
            return -1;
        }

        final ByteBuffer messageBuffer = ByteBuffer.allocate(messageLen);
        long curr = start + MessageUtils.HEADER_LEN;
        while (messageBuffer.hasRemaining()) {
            read = this.channel.read(messageBuffer);
            if (read < 0) {
                throw new IOException("文件在recover过程中被修改");
            }
            curr += read;
        }
        if (CheckSum.crc32(messageBuffer.array()) != checksum) {
            return -1;
        } else {
            return next;
        }
    }



    /**
     * just for test
     *
     * @param newValue
     */
    void setSizeInBytes(final long newValue) {
        this.sizeInBytes.set(newValue);
    }

    /**
     * just for test
     *
     * @param waterMark
     */
    void setHighWaterMarker(final long waterMark) {
        this.highWaterMark.set(waterMark);
    }

    /**
     * Just for test
     *
     * @return
     */
    long getOffset() {
        return this.offset;
    }

    /**
     * Just for test
     *
     * @return
     */
    public long getSizeInBytes() {
        return this.sizeInBytes.get();
    }

    public boolean isMutable() {
        return this.mutable;
    }
    public void setMutable(final boolean mutable) {
        this.mutable = mutable;
    }
    FileChannel getFileChannel() {
        return this.channel;
    }
    FileChannel channel() {
        return this.channel;
    }
}