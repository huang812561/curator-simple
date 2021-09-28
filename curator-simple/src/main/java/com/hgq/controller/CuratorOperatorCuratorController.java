package com.hgq.controller;

import com.hgq.CuratorZkTemplate;
import com.hgq.CuratorZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @ClassName com.hgq.controller.CuratorOperatorCuratorController
 * @Description: curator 测试类
 * @Author: hgq
 * @Date: 2021-09-24 17:49
 * @Version: 1.0
 */
@RestController
@Slf4j
public class CuratorOperatorCuratorController {

    @Autowired
    private CuratorZkTemplate curatorZkTemplate;

    @Autowired
    private CuratorZkUtil curatorZkUtil;

    String lockKey = "/curator-simple/aa";

    /**
     * 测试锁
     *
     * @return
     */
    @RequestMapping("hello")
    public String hello() {
        String lockKey = "/curator-simple";
        InterProcessLock lock = null;
        try {
            lock = curatorZkTemplate.getSharedLock(lockKey);
            lock.acquire();
            System.out.println(String.format("thread name:{%s} 获取锁 success！", Thread.currentThread().getName()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != lock) {
                try {
                    lock.release();
                    System.out.println(String.format("thread name:{%s} 释放锁 success！", Thread.currentThread().getName()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return "success";
    }

    /**
     * 测试创建节点
     *
     * @return
     */
    @RequestMapping("hello2")
    public String hello2() {
        String lockKey = "/curator-simple";
        if (StringUtils.isNotBlank(curatorZkUtil.createNodeData(lockKey, "hello2"))) {
            System.out.println("create node success");
            return "success";
        } else {
            System.out.println("create node failure");
        }
        return "failure";
    }

    /**
     * 更新节点数据
     *
     * @return
     */
    @RequestMapping("hello3")
    public String hello3() {
        curatorZkUtil.orSetNodeData(lockKey, "hello hgq");
        curatorZkUtil.setNodeData(lockKey, "value hello3");
        if (null != curatorZkUtil.setNodeDataWithVersion(lockKey, "value hello3", 10)) {
            System.out.println("update node data success");
            return "success";
        } else {
            System.out.println("update node data failure");
        }

        curatorZkUtil.orSetNodeDataWithVersion(lockKey, "hello hgq", 100);
        return "failure";

    }

    /**
     * 测试删除节点
     *
     * @return
     */
    @RequestMapping("hello4")
    public String hello4() {
        curatorZkUtil.createNodeData(lockKey, "hello2");
        curatorZkUtil.delNode(lockKey);
        String returnNode = curatorZkUtil.createTempNodeDataWithProtection(lockKey, "hello4");
        Stat stat = curatorZkUtil.getNodeStat(returnNode);
        if (null != stat) {
            curatorZkUtil.deleChildNode(returnNode);
        }
        curatorZkUtil.createNodeData(lockKey, "hello2");
        curatorZkUtil.deleChildNode(lockKey);
        curatorZkUtil.createNodeData(lockKey, "hello2");
        curatorZkUtil.delNodeWithVersion(lockKey, 10);
        curatorZkUtil.createNodeData(lockKey, "hello2");
        Stat stat2 = curatorZkUtil.getNodeStat(lockKey);
        if (null != stat2) {
            curatorZkUtil.deleChildNodeWithVersion(lockKey, stat.getVersion());
        }
        return "success";
    }

    /**
     * 测试事务
     *
     * @return
     */
    @RequestMapping("hello5")
    public String hello5() {
        // 定义几个基本操作
        try {
            //单事务操作
            CuratorOp createOp = curatorZkUtil.getTransaction().create().forPath(lockKey, "some data".getBytes());
            CuratorOp setDataOp = curatorZkUtil.getTransaction().setData().forPath(lockKey, "other data".getBytes());
            CuratorOp deleteOp = curatorZkUtil.getTransaction().delete().forPath(lockKey);

            // 多事务处理，执行结果
            List<CuratorTransactionResult> results = curatorZkUtil.getMultiTransaction().forOperations(createOp, setDataOp, deleteOp);

            // 遍历输出结果
            for (CuratorTransactionResult result : results) {
                System.out.println("执行结果是： " + result.getForPath() + "--" + result.getType() + "--resultStat--" + result.getError());
            }
        } catch (Exception e) {
            System.out.println(e);
            return "failure";
        }
        return "success";

    }

    /**
     * 测试监听
     *
     * @return
     */
    @RequestMapping("hello6")
    public String hello6() throws InterruptedException {
        String lockKey = "/curator-simple";
        //curatorZkUtil.addListenerWithPathChildrenCache(lockKey);
        curatorZkUtil.createNodeWithMode(lockKey, CreateMode.PERSISTENT,"hgq");
        /*curatorZkUtil.registerWatcherAllNodeChanged(lockKey, new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                switch (type) {
                    case NODE_CHANGED:
                        log.info("节点更新,path={},oldData={},newData={}", lockKey, new String(oldData.getData(), StandardCharsets.UTF_8), new String(data.getData(), StandardCharsets.UTF_8));
                        break;
                    case NODE_CREATED:
                        log.info("节点创建,path={},oldData={},newData={}", lockKey, new String(oldData.getData(), StandardCharsets.UTF_8), new String(data.getData(), StandardCharsets.UTF_8));
                        break;
                    case NODE_DELETED:
                        log.info("节点删除,path={},oldData={},newData={}", lockKey, new String(oldData.getData(), StandardCharsets.UTF_8), new String(data.getData(), StandardCharsets.UTF_8));
                        break;
                    default:
                        break;
                }
            }
        });*/

        CuratorCacheListener curatorCacheListener = CuratorCacheListener.builder()
                .forInitialized(() -> System.out.println(" Curator Cache initialized "))
                .forCreates(node -> System.out.println(" 节点创建，path:" + node.getPath() + ",data:" + new String(node.getData(), StandardCharsets.UTF_8)))
                .forChanges((oldNode, node) -> System.out.println(" 节点更新，oldPath:" + oldNode.getPath() + ",path:" + node.getPath() + ",oldData" + new String(oldNode.getData(), StandardCharsets.UTF_8) + ",data:" + new String(node.getData(), StandardCharsets.UTF_8)))
                .forDeletes(node -> System.out.println(" 节点删除，path:" + node.getPath() + ",data:" + new String(node.getData(), StandardCharsets.UTF_8)))
                .build();
        curatorZkUtil.registerWatcherAllNodeChanged(lockKey,curatorCacheListener);
        return "success";
    }


}
