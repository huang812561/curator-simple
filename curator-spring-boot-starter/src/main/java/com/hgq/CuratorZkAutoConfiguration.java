package com.hgq;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @ClassName com.hgq.CuratorZkAutoConfiguration
 * @Description: Curator 客户端连接实例化
 * @Author: hgq
 * @Date: 2021-09-23 16:48
 * @Version: 1.0
 */
@Slf4j
@Configuration
//@ConditionalOnProperty(prefix = "curator", name = "enabled", havingValue = "true")
@ConditionalOnClass(value = {CuratorFramework.class, RetryPolicy.class, InterProcessLock.class, ZooKeeper.class})
@EnableConfigurationProperties(value = CuratorZkProperties.class)
public class CuratorZkAutoConfiguration {

    /**
     * 重试策略，初始时间1S，重试次数10次
     *
     * @param properties
     * @return
     */
    @Bean
    @ConditionalOnMissingBean
    public RetryPolicy retryPolicy(CuratorZkProperties properties) {
        return new ExponentialBackoffRetry(properties.getBaseSleepTimeMs(), properties.getMaxRetries(), properties.getMaxSleepMs());
    }

    /**
     * 使用Fluent风格的API来创建Curator实例
     *
     * @param properties
     * @param retryPolicy
     * @return
     */
    @Bean
    @ConditionalOnMissingBean
    public CuratorFramework client(CuratorZkProperties properties, RetryPolicy retryPolicy) {
        // 1、创建连接实例
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(properties.getConnectString())   //配置zk服务器IP port
                .connectionTimeoutMs(properties.getConnectionTimeoutMs())   //设置连接超时时间
                .sessionTimeoutMs(properties.getSessionTimeoutMs()) //会话超时时间
                .canBeReadOnly(properties.isCanBeReadOnly())
                .authorization(CollectionUtils.isEmpty(properties.getAuthInfoList()) ? new ArrayList<>()
                        : properties.getAuthInfoList().stream().map(authInfo -> {
                    return new AuthInfo(authInfo.getScheme(), authInfo.getAuth().getBytes());
                }).collect(Collectors.toList()))
                .ensembleTracker(properties.isWithEnsembleTracker())//设置监听配置
                .namespace(properties.getNamespace())//设置命名空间
                .retryPolicy(retryPolicy)   //设置重连机制
                .build();

        //添加重连监听
        client.getConnectionStateListenable().addListener((curatorFramework, connectionState) -> {
            switch (connectionState) {
                //Sent for the first successful connection to the server
                case CONNECTED:
                    log.info("middleware schedule init server connected {}", properties.getConnectString());
                    break;
                //A suspended, lost, or read-only connection has been re-established
                case RECONNECTED:
                    log.info("middleware schedule init server reconnected {}", properties.getConnectString());
                    break;
                default:
                    break;
            }
        });
        client.start();
        log.info("curator connected---------> " + client.getState());
        return client;
    }

    /**
     * 创建 Curator 分布式锁实例
     *
     * @param properties
     * @param client
     * @param retryPolicy
     * @return
     */
    @Bean
    public CuratorZkTemplate curatorZkTemplate(CuratorZkProperties properties, CuratorFramework client, RetryPolicy retryPolicy) {
        return new CuratorZkTemplate(client, retryPolicy, properties.getSessionTimeoutMs());
    }

    @Bean
    public CuratorZkUtil curatorZkUtil(CuratorFramework client) {
        return new CuratorZkUtil(client);
    }


}
