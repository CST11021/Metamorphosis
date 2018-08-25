package com.taobao.metamorphosis.client.consumer;

import com.taobao.metamorphosis.Message;

/**
 * Created with IntelliJ IDEA.
 * User: dennis (xzhuang@avos.com)
 * Date: 13-2-5
 * Time: 上午11:18
 */
public interface RejectConsumptionHandler {
    /**
     * Method that may be invoked by a MessageConsumer when receiveMessages cannot process a message when retry too many times.
     * 当消费者尝试多次都无法消费消息时，会调用该方法
     *
     * @param message
     * @param messageConsumer
     */
    public void rejectConsumption(Message message, MessageConsumer messageConsumer);
}
