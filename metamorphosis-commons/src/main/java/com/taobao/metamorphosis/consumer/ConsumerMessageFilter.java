package com.taobao.metamorphosis.consumer;

import com.taobao.metamorphosis.Message;


/**
 * 用于消费者过滤消息，在某些场景下你可能只想消费一个topic下满足一定要求的消息
 * 
 * @since 1.4.6
 * @author dennis<killme2008@gmail.com>
 * 
 */
public interface ConsumerMessageFilter {
    /**
     * Test if the filter can accept a metaq message.Any exceptions threw by
     * this method means the message is not accepted.This method must be
     * thread-safe.
     * 
     * @param group
     * @param message
     * @return true if it accepts.
     */
    public boolean accept(String group, Message message);
}
