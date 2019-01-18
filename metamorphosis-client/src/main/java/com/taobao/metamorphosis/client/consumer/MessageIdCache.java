package com.taobao.metamorphosis.client.consumer;

/**
 * Message id cache to prevent duplicated messages for the same consumer group.
 * 
 * @author dennis<killme2008@gmail.com>
 * @since 1.4.6
 * 
 */
public interface MessageIdCache {

    /**
     * 添加一个key到缓存
     * 
     * @param key
     * @param exists
     */
    public void put(String key, Byte exists);


    /**
     * Get value from cache,it the item is exists,it must be returned.
     * 
     * @param key
     * @return
     */
    public Byte get(String key);
}
