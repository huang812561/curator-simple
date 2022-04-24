package com.hgq;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName com.hgq.CuratorZkDistributedLock
 * @Description: 自定义分布式监听器
 * @Author: hgq
 * @Date: 2021-09-24 16:33
 * @Version: 1.0
 */
public class CuratorZkDistributedLock implements Watcher {

    private CuratorFramework client;
    private String locksRoot = "/locks";
    private String waitNode;
    private String lockNode;
    private CountDownLatch latch;
    private CountDownLatch connectedLatch = new CountDownLatch(1);
    private int sessionTimeout = 30000;

    public CuratorZkDistributedLock(CuratorFramework client, int sessionTimeout) {
        try {
            this.client = client;
            this.sessionTimeout = sessionTimeout;

            connectedLatch.wait();
        } catch (InterruptedException e) {
            throw new CuratorLockException(BusinessMsgEnum.SYSTEM_EXCEPTION);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        /**
         * 处于连接zk状态时
         */
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedLatch.countDown();
            return;
        }
        if (null != this.latch) {
            this.latch.countDown();
        }
    }


    public void acquireLock(String lockKey) {
        try {
            if (this.tryLock(lockKey)) {
                return;
            } else {
                waitForLock(waitNode, sessionTimeout);
            }
        } catch (Exception e) {
            throw new CuratorLockException(e);
        }
    }

    private void waitForLock(String waitNode, int sessionTimeout) {
    }

    private boolean tryLock(String lockKey) {
        try {
            String lockPath = locksRoot + "/" + lockKey;
            //1. 创建临时有序节点
            lockNode = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(lockPath, new byte[0]);
            List<String> locks = client.getChildren().forPath(locksRoot);
            Collections.sort(locks);

            //2. 如果是最小的节点，则表示获取到锁
            if (lockNode.equals(locksRoot + "/" + locks.get(0))) {
                return true;
            }

            //3. 如果不是最小的节点，找到比自己小的节点
            int previousLockIndex = -1;
            for (int i = 0; i < locks.size(); i++) {
                if (lockNode.equals(locksRoot + "/" + locks.get(i))) {
                    previousLockIndex = 1;
                    break;
                }
            }
            this.waitNode = locksRoot+"/"+locks.get(previousLockIndex);

        } catch (KeeperException e) {
            throw new CuratorLockException(e);
        } catch (InterruptedException e) {
            throw new CuratorLockException(e);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }


    private boolean waitForLock(String waitNode, long waitTime) throws Exception {

        Stat stat = client.checkExists().forPath(locksRoot + "/" + waitNode);
        if (stat != null) {
            this.latch = new CountDownLatch(1);
            this.latch.await(waitTime, TimeUnit.MILLISECONDS);
            this.latch = null;
        }
        return true;
    }

    public boolean unlock() {
        try {
            // 删除/locks/10000000000节点
            client.delete().forPath(lockNode);
            lockNode = null;
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }



}
