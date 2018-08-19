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

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 抓取请求队列：用于维护从MQ服务器抓取的消息
 *
 * 稳定排序的delay queue，线程安全
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
class FetchRequestQueue {

    /** 用于保存从MQ服务器抓取的消息 */
    private final LinkedList<FetchRequest> queue = new LinkedList<FetchRequest>();

    /** 队列的显式锁 */
    private final Lock lock = new ReentrantLock();

    /**  */
    private final Condition available = this.lock.newCondition();

    /**
     * 表示当前正在从队列获取请求对象的线程，如果该属性不为null，说明当前有线程正在从队列获取元素
     *
     * Thread designated to wait for the element at the head of the queue. This
     * variant of the Leader-Follower pattern
     * (http://www.cs.wustl.edu/~schmidt/POSA/POSA2/) serves to minimize
     * unnecessary timed waiting. When a thread becomes the leader, it waits
     * only for the next delay to elapse, but other threads await indefinitely.
     * The leader thread must signal some other thread before returning from
     * take() or poll(...), unless some other thread becomes leader in the
     * interim. Whenever the head of the queue is replaced with an element with
     * an earlier expiration time, the leader field is invalidated by being
     * reset to null, and some waiting thread, but not necessarily the current
     * leader, is signalled. So waiting threads must be prepared to acquire and
     * lose leadership while waiting.
     */
    private Thread leader = null;

    /**
     * 从队列中获取一个请求对象
     * @return
     * @throws InterruptedException
     */
    public FetchRequest take() throws InterruptedException {
        final Lock lock = this.lock;
        // 如果当前线程未被中断，则获取锁
        lock.lockInterruptibly();
        try {
            for (;;) {
                // 获取抓取队列中的第一个抓取请求的对象
                FetchRequest first = this.queue.peek();
                if (first == null) {
                    // 如果当前队列一个请求对象都没有，则线程进行等待
                    this.available.await();
                }
                else {
                    // 当前该抓取请求的对象被保存到队列后，通getDelay()来实现延迟队列，即该请求对象被保存多久后才能从队列里取出
                    long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delay <= 0) {
                        return this.queue.poll();
                    }
                    else if (this.leader != null) {
                        // 设置当前线程进入等待
                        this.available.await();
                    }
                    else {
                        Thread thisThread = Thread.currentThread();
                        this.leader = thisThread;
                        try {
                            // 调用该方法的前提是，当前线程已经成功获得与该条件对象绑定的重入锁，否则调用该方法时会抛出IllegalMonitorStateException。
                            // nanosTimeout指定该方法等待信号的的最大时间（单位为纳秒）。若指定时间内收到signal()或signalALL()则返回nanosTimeout减去已经等待的时间；
                            // 若指定时间内有其它线程中断该线程，则抛出InterruptedException并清除当前线程的打断状态；若指定时间内未收到通知，则返回0或负数。
                            this.available.awaitNanos(delay);
                        }
                        finally {
                            if (this.leader == thisThread) {
                                this.leader = null;
                            }
                        }
                    }
                }
            }
        }
        finally {
            if (this.leader == null && this.queue.peek() != null) {
                // 随机唤醒争抢锁队列中的一个线程
                this.available.signal();
            }
            lock.unlock();
        }
    }

    /**
     * 将抓取请求添加到请求队列中
     * @param e
     */
    public void offer(FetchRequest e) {
        final Lock lock = this.lock;
        lock.lock();
        try {
            /**
             * A request is not referenced by this queue,so we don't want to add it.
             * 该请求不依赖队列，因此我们不添加它。
             */
            if (e.getRefQueue() != null && e.getRefQueue() != this) {
                return;
            }

            // 设置这个请求要保存的队列
            e.setRefQueue(this);
            this.queue.offer(e);
            Collections.sort(this.queue);
            if (this.queue.peek() == e) {
                this.leader = null;
                // 随机唤醒争抢锁队列中的一个线程
                this.available.signal();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * 返回当前队列大小
     * @return
     */
    public int size() {
        final Lock lock = this.lock;
        lock.lock();
        try {
            return this.queue.size();
        }
        finally {
            lock.unlock();
        }
    }

}