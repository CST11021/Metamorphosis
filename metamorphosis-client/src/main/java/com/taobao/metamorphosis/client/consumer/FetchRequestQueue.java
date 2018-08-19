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
 * ץȡ������У�����ά����MQ������ץȡ����Ϣ
 *
 * �ȶ������delay queue���̰߳�ȫ
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
class FetchRequestQueue {

    /** ���ڱ����MQ������ץȡ����Ϣ */
    private final LinkedList<FetchRequest> queue = new LinkedList<FetchRequest>();

    /** ���е���ʽ�� */
    private final Lock lock = new ReentrantLock();

    /**  */
    private final Condition available = this.lock.newCondition();

    /**
     * ��ʾ��ǰ���ڴӶ��л�ȡ���������̣߳���������Բ�Ϊnull��˵����ǰ���߳����ڴӶ��л�ȡԪ��
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
     * �Ӷ����л�ȡһ���������
     * @return
     * @throws InterruptedException
     */
    public FetchRequest take() throws InterruptedException {
        final Lock lock = this.lock;
        // �����ǰ�߳�δ���жϣ����ȡ��
        lock.lockInterruptibly();
        try {
            for (;;) {
                // ��ȡץȡ�����еĵ�һ��ץȡ����Ķ���
                FetchRequest first = this.queue.peek();
                if (first == null) {
                    // �����ǰ����һ���������û�У����߳̽��еȴ�
                    this.available.await();
                }
                else {
                    // ��ǰ��ץȡ����Ķ��󱻱��浽���к�ͨgetDelay()��ʵ���ӳٶ��У�����������󱻱����ú���ܴӶ�����ȡ��
                    long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delay <= 0) {
                        return this.queue.poll();
                    }
                    else if (this.leader != null) {
                        // ���õ�ǰ�߳̽���ȴ�
                        this.available.await();
                    }
                    else {
                        Thread thisThread = Thread.currentThread();
                        this.leader = thisThread;
                        try {
                            // ���ø÷�����ǰ���ǣ���ǰ�߳��Ѿ��ɹ���������������󶨵���������������ø÷���ʱ���׳�IllegalMonitorStateException��
                            // nanosTimeoutָ���÷����ȴ��źŵĵ����ʱ�䣨��λΪ���룩����ָ��ʱ�����յ�signal()��signalALL()�򷵻�nanosTimeout��ȥ�Ѿ��ȴ���ʱ�䣻
                            // ��ָ��ʱ�����������߳��жϸ��̣߳����׳�InterruptedException�������ǰ�̵߳Ĵ��״̬����ָ��ʱ����δ�յ�֪ͨ���򷵻�0������
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
                // ������������������е�һ���߳�
                this.available.signal();
            }
            lock.unlock();
        }
    }

    /**
     * ��ץȡ������ӵ����������
     * @param e
     */
    public void offer(FetchRequest e) {
        final Lock lock = this.lock;
        lock.lock();
        try {
            /**
             * A request is not referenced by this queue,so we don't want to add it.
             * �������������У�������ǲ��������
             */
            if (e.getRefQueue() != null && e.getRefQueue() != this) {
                return;
            }

            // �����������Ҫ����Ķ���
            e.setRefQueue(this);
            this.queue.offer(e);
            Collections.sort(this.queue);
            if (this.queue.peek() == e) {
                this.leader = null;
                // ������������������е�һ���߳�
                this.available.signal();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * ���ص�ǰ���д�С
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