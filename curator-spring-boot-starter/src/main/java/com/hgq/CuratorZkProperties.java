package com.hgq;

import lombok.Data;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @ClassName com.hgq.CuratorZkProperties
 * @Description: TODO
 * @Author: hgq
 * @Date: 2021-09-23 16:18
 * @Version: 1.0
 */
@ConfigurationProperties(prefix = CuratorZkProperties.PREFIX)
@Data
public class CuratorZkProperties {
    public static final String PREFIX = "curator";
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

    /**
     * As Zookeeper is a shared space, users of a given cluster should stay within
     * a pre-defiend namespace. If a namespace is set here, all paths will get pre-pended with the namespace
     */
    private String namespace;

    /**
     * connectString – list of servers to connect to
     * 192.168.1.1:2100,192.168.1.2:2101,192.168.1.3:2102
     */
    private String connectString;

    /**
     * connectionTimeoutMs – connection timeout
     * default value : 30S
     */
    private int connectionTimeoutMs = 30000;

    /**
     * sessionTimeoutMs – session timeout
     * default value : 30S
     */
    private int sessionTimeoutMs = 30000;

    /**
     * time to wait during close to join background threads
     */
    private int maxCloseWaitMs;

    /**
     * Set a timeout for {@link CuratorZookeeperClient#close(int)}  }.
     * The default is 0, which means that this feature is disabled.
     */
    private int waitForShutdownTimeoutMs;
    /**
     * new simulated session expiration percentage
     */
    private int simulatedSessionExpirationPercent = -1;

    /**
     * if true, allow ZooKeeper client to enter
     * read only mode in case of a network partition. See
     * {@link ZooKeeper#ZooKeeper(String, int, Watcher, long, byte[], boolean)}
     * for details
     */
    private boolean canBeReadOnly = true;

    /**
     * baseSleepTimeMs – initial amount of time to wait between retries
     */
    private int baseSleepTimeMs = 1000;

    /**
     * maxRetries – max number of times to retry
     */
    private int maxRetries = 10;

    /**
     * * maxSleepMs – max time in ms to sleep on each retry
     */
    private int maxSleepMs = DEFAULT_MAX_SLEEP_MS;

    /**
     * Allows to configure if the ensemble configuration changes will be watched.
     * The default value is {@code true}.
     */
    private boolean withEnsembleTracker = true;

    /**
     * authInfo
     */
    private List<CuratorAuthInfo> authInfoList;

    @Data
    public class CuratorAuthInfo {
        private String scheme;
        private String auth;

    }

}
