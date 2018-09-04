package com.maple.zookeeper.maple;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 *   1） 首先创建根节点/cluster，并创建自身子节点，以 /cluster/w- 为前缀，使用临时自动编号节点模式创建节点

     2）获取/cluster的所有子节点并排序，当发现自身是第一个节点时，则自我选举为leader，否则认定为follower

     3）注册监听事件，当/cluster里前一个节点有变动时，回到2）
 *
 *
 */
public class Leader extends ZooKeeper implements Runnable, Watcher {

    public static final String NODE_NAME = "/cluster";
    public String znode;
    private boolean leader;

    public Leader(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
    }

    public boolean register() throws InterruptedException, KeeperException {
        if (this.exists(NODE_NAME, null) == null) {
            this.create(NODE_NAME, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        znode = this.create(NODE_NAME + "/w-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        znode = znode.replace(NODE_NAME + "/", "");
        String node = watchPrevious();
        if (node.equals(znode)) {
            System.out.println("nobody here ,i am leader");
            leader = true;
        } else {
            System.out.println("i am watching");
        }
        return true;
    }

    private String watchPrevious() throws InterruptedException, KeeperException {
        List<String> works = this.getChildren(NODE_NAME, this);
        Collections.sort(works);
        System.out.println(works);
        int i = 0;
        for (String work : works) {
            if (znode.equals(work)) {
                if (i > 0) {
                    //this.getData(NODE_NAME + "/" + works.get(i - 1), this, null);
                    return works.get(i - 1);
                }
                return works.get(0);
            }
        }
        return "";

    }

    @Override
    public void run() {
        try {
            this.register();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
        while (true) {
            try {
                if (leader) {
                    System.out.println("leading");
                } else {
                    System.out.println("following");
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public static void main(String[] args) {
        try {
            String hostPort = "10.16.73.22,10.16.73.12,10.16.73.13";
            new Thread(new Leader(hostPort, 3000, null)).start();
        } catch (IOException e) {
        }
    }


    @Override
    public void process(WatchedEvent event) {
        String t = String.format("hello event! type=%s, stat=%s, path=%s", event.getType(), event.getState(), event.getPath());
        System.out.println(t);
        System.out.println("hello ,my cluster id is :" + znode);
        String node = "";
        try {
            node = this.watchPrevious();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }

        if (node.equals(znode)) {
            System.out.println("process: nobody here ,i am leader");
            leader = true;
        } else {
            System.out.println("process: i am watching");
        }
    }
}
