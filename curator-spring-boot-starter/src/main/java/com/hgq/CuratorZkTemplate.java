package com.hgq;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicValue;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;

import java.util.Arrays;

/**
 * @ClassName com.hgq.CuratorZkTemplate
 * @Description: Curator分布式锁
 * @Author: hgq
 * @Date: 2021-09-24 09:44
 * @Version: 1.0
 */
public class CuratorZkTemplate {
    private CuratorFramework client;
    private RetryPolicy retryPolicy;
    private int sessionTimeout = 30000;

    public CuratorZkTemplate(CuratorFramework client, RetryPolicy retryPolicy, int sessionTimeout) {
        this.client = client;
        this.retryPolicy = retryPolicy;
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * 1. 共享可重入锁 ==> InterProcessMutex
     *
     * @param lockKey
     * @return
     */
    public InterProcessLock getSharedReentrantLock(String lockKey) {
        return new InterProcessMutex(client, lockKey);
    }

    /**
     * 2. 共享不可重入锁(排它锁) ==>  InterProcessSemaphoreMutex
     *
     * @param lockKey
     * @return
     */
    public InterProcessLock getSharedLock(String lockKey) {
        return new InterProcessSemaphoreMutex(client, lockKey);
    }

    /**
     * 3. 共享可重入读写锁  ===》InterProcessReadWriteLock
     *
     * @param lockKey
     * @return
     */
    public InterProcessReadWriteLock getSharedReentrantReadWriteLock(String lockKey) {
        return new InterProcessReadWriteLock(client, lockKey);
    }

    /**
     * 4. 共享信号量锁 ===》InterProcessSemaphoreV2
     * 以公平锁的方式进行实现
     *
     * @param lockKey
     * @return
     */
    public InterProcessSemaphoreV2 getSharedSemaphoreLock(String lockKey) {
        return new InterProcessSemaphoreV2(client, lockKey, 1);
    }

    /**
     * 5. 创建多重共享锁
     * 将多个锁作为单个实体管理的容器
     *
     * @param lockKeys
     * @return
     */
    public InterProcessMultiLock getSharedSemaphore(InterProcessLock... lockKeys) {
        return new InterProcessMultiLock(Arrays.asList(lockKeys));
    }

/*    public InterProcessMultiLock getSharedMultiLock(String... lockKeys) {
        return new InterProcessMultiLock(client, Arrays.asList(lockKeys));
    }*/

    /**
     * 1. 分布式操作    =======》 DistributedBarrier
     * 在分布式系统中，可以使用栅栏，对多个节点上的任务进行阻塞等待；直到满足某个定制的条件，所有的节点才可以继续执行下一步任务。
     * <p>
     * 通过调用DistributedBarrier.setBarrier()方法来完成Barrier的设置，并通过调用DistributedBarrier.waitOnBarrier()方法来等待Barrier的释放。
     * 然后在主线程中，通过调用DistributedBarrier.removeBarrier()方法来释放Barrier，同时触发所有等待该Barrier的5个线程同时进行各自的逻辑。
     *
     * @param barrierPath
     * @return
     */
    public DistributedBarrier getBarrier(String barrierPath) {
        return new DistributedBarrier(client, barrierPath);
    }


    /**
     * 2. 双重栅栏  =====》 DistributedDoubleBarrier
     * 双重栅栏能够让客户端在任务的开始和结束阶段更好的同步控制。 当有足够的任务已经进入到栅栏后，一起开始，一旦任务完成则离开栅栏。
     *
     * @param barrierPath
     * @param memberQty
     * @return
     */
    public DistributedDoubleBarrier getDoubleBarrier(String barrierPath, int memberQty) {
        return new DistributedDoubleBarrier(client, barrierPath, memberQty);
    }

    /**
     * 分布式计数器
     * 能在分布式环境下实现原子自增
     *
     * @param lockKey
     * @return
     */
    public DistributedAtomicInteger getAtomicInteger(String lockKey) {
        return new DistributedAtomicInteger(client, lockKey, retryPolicy);
    }

    /**
     * 分布式计数器
     * 能在分布式环境下实现原子自增
     *
     * @param lockKey
     * @return
     */
    public DistributedAtomicLong getAtomicLong(String lockKey) {
        return new DistributedAtomicLong(client, lockKey, retryPolicy);
    }

    public DistributedAtomicValue getAtomicValue(String lockValue) {
        return new DistributedAtomicValue(client, lockValue, retryPolicy);
    }


    /**
     * curator实现了分布式场景下的阻塞队列
     *
     * @param lockKey
     * @return
     */
    public SimpleDistributedQueue getQueue(String lockKey){
        return new SimpleDistributedQueue(client,lockKey);
    }

    /**
     * 获取重试策略
     *
     * @return
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * 获取Curator实例会话
     * @return
     */
    public CuratorFramework getClient() {
        return client;
    }

}
