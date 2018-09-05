package com.maple.zookeeper.zookeeper.watcher;

import org.apache.zookeeper.Watcher;

/**
 * @author maple 2018.09.05 上午9:20
 */
public class ZkServiceInfo {

    private Watcher watcher = new ZkWatcher(this);

    final String service;

    public int counter = 1;
    public int configCounter = 1;


    public ZkServiceInfo(String service) {
        this.service = service;
    }


    public String getService() {
        return service;
    }


    public Watcher getWatcher() {
        return watcher;
    }

}
