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
package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 查询最近有效的offset 格式： offset topic group partition offset opaque\r\n
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class OffsetCommand extends AbstractRequestCommand {

    static final long serialVersionUID = -1L;

    /**
     * 表示要获取偏移量的所在分区
     */
    private final int partition;

    /**
     * 表示要获取偏移量的消费者分组，不通分组偏移量不一样
     */
    private final String group;

    /**
     * 客户端上传的偏移量
     */
    private final long offset;

    public OffsetCommand(final String topic, final String group, final int partition, final long offset, final Integer opaque) {
        super(topic, opaque);
        this.group = group;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public IoBuffer encode() {
        // 分配该请求命令的内存大小
        final IoBuffer buf = IoBuffer.allocate(13
                + this.getTopic().length()
                + this.getGroup().length()
                + ByteUtils.stringSize(this.partition)
                + ByteUtils.stringSize(this.offset)
                + ByteUtils.stringSize(this.getOpaque()));

        ByteUtils.setArguments(buf, MetaEncodeCommand.OFFSET_CMD, this.getTopic(), this.getGroup(), this.getPartition(), this.offset, this.getOpaque());
        // Buffer 中的 flip() 方法涉及到 Buffer 中的capacity、position、limit三个概念。
        // capacity：在读/写模式下都是固定的，就是我们分配的缓冲大小（容量）。
        // position：类似于读/写指针，表示当前读(写)到什么位置。
        // limit：在写模式下表示最多能写入多少数据，此时和capacity相同。在读模式下表示最多能读多少数据，此时和缓存中的实际数据大小相同。
        // flip()：Buffer有两种模式，写模式和读模式。在写模式下调用flip()之后，Buffer从写模式变成读模式。
        // 那么limit就设置成了position当前的值(即当前写了多少数据)，postion会被置为0，以表示读操作从缓存的头开始读，mark置为-1。
        // 也就是说调用flip()之后，读/写指针position指到缓冲区头部，并且设置了最多只能读出之前写入的数据长度(而不是整个缓存的容量大小)。
        buf.flip();
        return buf;
    }

    public int getPartition() {
        return this.partition;
    }
    public String getGroup() {
        return this.group;
    }
    public long getOffset() {
        return this.offset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (this.group == null ? 0 : this.group.hashCode());
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + this.partition;
        return result;
    }
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final OffsetCommand other = (OffsetCommand) obj;
        if (this.group == null) {
            if (other.group != null) {
                return false;
            }
        }
        else if (!this.group.equals(other.group)) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.partition != other.partition) {
            return false;
        }
        return true;
    }

}