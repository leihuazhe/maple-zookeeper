package com.maple.zookeeper.maple;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKWatch implements Watcher{
    private Logger LOGGER = LoggerFactory.getLogger(ZKWatch.class);
    private String znode;

    public String getZnode() {
        return znode;
    }

    public void setZnode(String znode) {
        this.znode = znode;
    }

    public ZKWatch(String znode){
        this.znode = znode;
    }


    @Override
    public void process(WatchedEvent event) {


        }
}
