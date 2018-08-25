package com.taobao.metamorphosis.client.extension.spring;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * Messge body object converter.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.5
 * @param <T>
 */
public interface MessageBodyConverter<T> {
    /**
     * Convert a message object to byte array.
     * 将消息对象转为byte
     * 
     * @param body
     * @return
     * @throws MetaClientException
     */
    public byte[] toByteArray(T body) throws MetaClientException;


    /**
     * Convert a byte array to message object.
     * 将byte类型的消息转为消息对象
     * 
     * @param bs
     * @return
     * @throws MetaClientException
     */
    public T fromByteArray(byte[] bs) throws MetaClientException;
}
