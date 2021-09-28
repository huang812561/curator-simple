package com.hgq;


import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName com.hgq.CuratorZkUtil
 * @Description: Curator 操作 Zookeeper 工具类
 * @Author: hgq
 * @Date: 2021-09-26 17:54
 * @Version: 1.0
 */
@Slf4j
public class CuratorZkUtil {

    private static CuratorFramework client;
    /**
     * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
     */
    ExecutorService pool = Executors.newFixedThreadPool(2);

    public CuratorZkUtil() {
    }

    public CuratorZkUtil(CuratorFramework curatorFramework) {
        client = curatorFramework;
    }

    public CuratorFramework getClient(){
        return client;
    }
    /**
     * 创建永久节点无数据（递归）
     *
     * @param path
     * @return
     */
    public static String createNode(String path) {
        try {
            //withProtectio():具有创建节点保护作用
            //client.create().forPath(path, data.getBytes());
            return client.create().creatingParentsIfNeeded().withProtection().forPath(path);
        } catch (Exception e) {
            log.error("创建永久节点异常,path={}", path, e);
        }
        return "";
    }


    /**
     * 创建永久节点并设置数据（递归）
     *
     * @param path
     * @param data
     * @return
     */
    public static String createNodeData(String path, String data) {
        try {
            return client.create().creatingParentsIfNeeded().forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("创建永久节点异常,path={},data:{}", path, data, e);
        }
        return "";
    }

    /**
     * 创建永久节点并设置数据（递归）
     * withProtection() : 在创建节点的时候设置序号 相当于给一个唯一id 这样重试的时候看如果节点创建成功就不会重复创建了
     *
     * @param path
     * @param data
     * @return
     */
    public static String createTempNodeDataWithProtection(String path, String data) {
        String result = "";
        try {
            result = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("创建永久节点异常,path={},data:{}", path, data, e);
        }
        return result;
    }

    /**
     * 创建节点(递归)
     *
     * @param path       节点路径
     * @param createMode 创建节点类型（CreateMode）
     *                   PERSISTENT             持久化节点
     *                   PERSISTENT_SEQUENTIAL  持久化有序节点
     *                   EPHEMERAL              临时节点，一旦创建这个节点当会话结束, 这个节点会被自动删除
     *                   EPHEMERAL_SEQUENTIAL   临时有序节点
     * @param data       设置数据
     * @return
     */
    public static String createNodeWithMode(String path, CreateMode createMode, String data) {
        String result = "";
        try {
            if (null == data) {
                data = "";
            }
            result = client.create().creatingParentsIfNeeded().withProtection().withMode(createMode).forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("创建节点异常,path={},mode={},data:{}", path, createMode, data, e);
        }
        return result;
    }

    /**
     * 校验节点是否存在
     *
     * @param path
     * @return
     */
    public static boolean checkExistsNode(String path) {
        try {
            Stat stat = client.checkExists().forPath(path);
            return null == stat ? false : true;
        } catch (Exception e) {
            log.error("校验节点是否存在出现异常,path={}", path, e);
            return false;
        }
    }

    /**
     * 查询节点下的所有子节点
     *
     * @param path
     * @return
     */
    public static List<String> queryChildPathList(String path) {
        List<String> childPathList = null;
        try {
            childPathList = client.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("查询节点下的所有子节点出现异常，path={}", path, e);
        }
        return childPathList;
    }

    /**
     * 获取节点版本信息
     *
     * @param path
     * @return
     */
    public Stat getNodeStat(String path) {
        Stat stat = null;
        try {
            stat = client.checkExists().forPath(path);
        } catch (Exception e) {
            log.error("获取节点版本信息出现异常,path={}", path, e);
        }
        return stat;
    }

    /**
     * 获取节点数据
     *
     * @param path
     * @return
     */
    public static String getNodeData(String path) {
        String result = "";
        try {
            result = new String(client.getData().forPath(path));
        } catch (Exception e) {
            log.error("查询节点的数据出现异常,path={}", path, e);
        }
        return result;
    }

    /**
     * 更新节点数据
     *
     * @param path
     * @param data
     * @return
     */
    public static Stat setNodeData(String path, String data) {
        try {
            return client.setData().forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("更新节点数据出现异常，path={},data={}", path, data, e);
        }
        return null;
    }

    /**
     * 带版本号的节点数据更新
     *
     * @param path
     * @param data
     * @param version 版本号
     * @return
     */
    public static Stat setNodeDataWithVersion(String path, String data, int version) {
        try {
            return client.setData().withVersion(version).forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("带版本号更新节点数据出现异常，path={},data={},version={}", path, data, version, e);
        }
        return null;
    }

    /**
     * 更新节点数据——没有节点则创建
     *
     * @param path
     * @param data
     * @return
     */
    public static boolean orSetNodeData(String path, String data) {
        try {
            client.create().orSetData().forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("更新节点数据(没有节点则创建)出现异常，path={},data={}", path, data, e);
            return false;
        }
        return true;
    }

    /**
     * 带版本号更新节点数据——没有节点则创建
     *
     * @param path
     * @param data
     * @return
     */
    public static boolean orSetNodeDataWithVersion(String path, String data, int version) {
        try {
            client.create().orSetData(version).forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("带版本号更新节点数据(没有节点则创建)出现异常，path={},data={}", path, data, e);
            return false;
        }
        return true;
    }

    /**
     * 删除节点
     *
     * @param path
     * @return
     */
    public static boolean delNode(String path) {
        try {
            client.delete().guaranteed().forPath(path);
        } catch (Exception e) {
            log.error("删除节点出现异常，path={}", path, e);
            return false;
        }
        return true;
    }

    /**
     * 级联删除节点
     *
     * @param path
     * @return
     */
    public static boolean deleChildNode(String path) {
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            log.error("级联删除节点出现异常，path={}", path, e);
            return false;
        }
        return true;
    }

    /**
     * 带版本号删除节点
     *
     * @param path
     * @return
     */
    public static boolean delNodeWithVersion(String path, int version) {
        try {
            client.delete().guaranteed().withVersion(version).forPath(path);
        } catch (Exception e) {
            log.error("带版本号删除节点出现异常，path={},version={}", path, version, e);
            return false;
        }
        return true;
    }

    /**
     * 带版本号级联删除节点
     *
     * @param path
     * @return
     */
    public static boolean deleChildNodeWithVersion(String path, int version) {
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded().withVersion(version).forPath(path);
        } catch (Exception e) {
            log.error("带版本号级联删除节点出现异常，path={},version={}", path, version, e);
            return false;
        }
        return true;
    }

    /**
     * 判断节点是否是持久化节点
     *
     * @param path
     * @return 2-节点不存在  | 1-是持久化 | 0-临时节点
     */
    public int isPersistentNode(String path) {
        try {
            Stat stat = client.checkExists().forPath(path);
            if (null == stat) {
                return 2;
            }
            if (stat.getEphemeralOwner() > 0) {
                return 1;
            }
            return 0;
        } catch (Exception e) {
            log.error("判断节点是否是持久化节点出现异常,path={}", path, e);
            return 2;
        }

    }

    /**
     * 开启事务
     *
     * @return 返回当前事务
     */
    public TransactionOp getTransaction() {
        client.transaction();
        return client.transactionOp();
    }

    /**
     * 开启多个事务
     *
     * @return
     */
    public CuratorMultiTransaction getMultiTransaction() {
        return client.transaction();
    }


    /**
     * PathChildrenCache 监听一个节点下子节点的创建、删除、更新操作
     *
     * @param path
     */
    @Deprecated
    public static void addListenerWithPathChildrenCache(String path) {
        try {
            PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, true);
            PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
                public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                    System.out.println("Receive Event:" + pathChildrenCacheEvent.getType());
                }
            };
            pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置Node Cache, 监控本节点的新增,删除,更新
     * 节点的update可以监控到, 如果删除会自动再次创建空节点
     * 本例子只是演示, 所以只是打印了状态改变的信息, 并没有在NodeCacheListener中实现复杂的逻辑
     *
     * @Param path 监控的节点路径, dataIsCompressed 数据是否压缩
     * 不可重入监听
     */
    @Deprecated
    public void setNodeCacheListener(String path, boolean dataIsCompressed) {
        try {
            NodeCache nodeCache = new NodeCache(client, path, dataIsCompressed);
            NodeCacheListener nodeCacheListener = new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    ChildData childData = nodeCache.getCurrentData();
                    log.info("ZNode节点状态改变, path={}", childData.getPath());
                    log.info("ZNode节点状态改变, data={}", childData.getData());
                    log.info("ZNode节点状态改变, stat={}", childData.getStat());
                }
            };
            nodeCache.getListenable().addListener(nodeCacheListener);
            nodeCache.start();
        } catch (Exception e) {
            log.error("创建NodeCache监听失败, path={}", path);
        }
    }


    /**
     * 设置Tree Cache, 监控本节点的新增,删除,更新
     * 节点的update可以监控到, 如果删除不会自动再次创建
     * 本例子只是演示, 所以只是打印了状态改变的信息, 并没有在NodeCacheListener中实现复杂的逻辑
     *
     * @Param path 监控的节点路径, dataIsCompressed 数据是否压缩
     * 可重入监听
     */
    @Deprecated
    public void setTreeCacheListener(final String path) {
        try {
            TreeCache treeCache = new TreeCache(client, path);
            TreeCacheListener treeCacheListener = new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                    ChildData data = event.getData();
                    if (data != null) {
                        switch (event.getType()) {
                            case NODE_ADDED:
                                log.info("[TreeCache]节点增加, path={}, data={}", data.getPath(), data.getData());
                                break;
                            case NODE_UPDATED:
                                log.info("[TreeCache]节点更新, path={}, data={}", data.getPath(), data.getData());
                                break;
                            case NODE_REMOVED:
                                log.info("[TreeCache]节点删除, path={}, data={}", data.getPath(), data.getData());
                                break;
                            default:
                                break;
                        }
                    } else {
                        log.info("[TreeCache]节点数据为空, path={}", data.getPath());
                    }
                }
            };
            treeCache.getListenable().addListener(treeCacheListener);
            treeCache.start();
        } catch (Exception e) {
            log.error("创建TreeCache监听失败, path={}", path);
        }

    }


    /**
     * 注册节点数据变化事件
     *
     * @param path              节点路径
     * @param curatorCacheListener 监听事件
     * @return 注册结果
     */
    public boolean registerWatcherNodeChanged(String path, CuratorCacheListener curatorCacheListener) {
        try {
            /**
             * 监听数据节点的变化情况
             */
            CuratorCache curatorCache = CuratorCache.build(client, path, CuratorCache.Options.SINGLE_NODE_CACHE);
            curatorCache.listenable().addListener(curatorCacheListener, pool);
            curatorCache.start();

        } catch (Exception e) {
            log.error("注册节点数据变化事件出现异常，path={}", path, e);
            return false;
        }

        return true;
    }

    /**
     * 注册子节点数据变化事件
     *
     * @param path              节点路径
     * @param curatorCacheListener 监听事件
     * @return 注册结果
     */
    public boolean registerWatcherAllNodeChanged(String path, CuratorCacheListener curatorCacheListener) {
        try {
            CuratorCache curatorCache = CuratorCache.build(client, path, CuratorCache.Options.SINGLE_NODE_CACHE);
            //注册监听
            curatorCache.listenable().addListener(curatorCacheListener, pool);
            curatorCache.start();
        } catch (Exception e) {
            log.error("注册子节点数据变化事件出现异常，path={}", path, e);
            return false;
        }
        return true;
    }


}
