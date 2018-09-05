package com.maple.zookeeper.zookeeper.watcher;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author maple 2018.09.04 下午3:58
 */
public class ZkWatcher implements Watcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkWatcher.class);
    private final ZkServiceInfo zkServiceInfo;


    public ZkWatcher(ZkServiceInfo zkServiceInfo) {
        this.zkServiceInfo = zkServiceInfo;
    }

    /**
     * @param event event
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            if (zkServiceInfo.counter++ <= 10) {
                LOGGER.info("{}:: syncRuntime节点 [{}]:{}子节点发生变化，重新获取信息",
                        getClass().getSimpleName(), zkServiceInfo.service, event.getPath());

                try {
                    ZookeeperClient.getInstance().syncZkRuntimeInfo(zkServiceInfo);
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }

        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            if (zkServiceInfo.configCounter++ <= 10) {
                LOGGER.info("{}:: syncConfig节点 [{}]: {} 节点内容发生变化，重新获取配置信息",
                        getClass().getSimpleName(), zkServiceInfo.service, event.getPath());

                ZookeeperClient.getInstance().syncZkConfigInfo(zkServiceInfo);
            }
        }

    }
}
