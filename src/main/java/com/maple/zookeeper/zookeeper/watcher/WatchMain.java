package com.maple.zookeeper.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author maple 2018.09.04 上午9:41
 */
public class WatchMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(WatchMain.class);

    public static void main(String[] args) throws InterruptedException, KeeperException {
        CountDownLatch cdl = new CountDownLatch(1);
        ZookeeperClient instance = ZookeeperClient.getInstance();

        for (int i = 0; i < 1000; i++) {
            instance.syncService(new ZkServiceInfo("TestService" + i));
        }

        cdl.await();
    }


}
