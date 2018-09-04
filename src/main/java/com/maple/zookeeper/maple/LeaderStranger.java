package com.maple.zookeeper.maple;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

public class LeaderStranger  implements Watcher{

    private Logger LOGGER = LoggerFactory.getLogger(LeaderStranger.class);
    /** 定义session失效时间 */
    private static final int SESSION_TIMEOUT = 10000;
    /** zookeeper服务器地址 */
    private static final String CONNECTION_ADDR = "127.0.0.1";
    /** zk父路径设置 */
    private static final String ROOT = "/cluster";
    /** zk变量 */
    private ZooKeeper zk = null;

    private String znode;

    private boolean leader;

    private String appName;

    /**用于等待zookeeper连接建立之后 通知阻塞程序继续向下执行 */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public LeaderStranger(String appName){
        this.appName = appName;
        init();
    }


    public void init(){
        this.releaseConnection();
        try {
            LOGGER.info("开始连接ZK服务器");
            zk = new ZooKeeper(CONNECTION_ADDR, SESSION_TIMEOUT, this);
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public boolean register() throws InterruptedException, KeeperException {
        if (zk.exists(ROOT, null) == null) {
            zk.create(ROOT, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        znode = zk.create(ROOT + "/" + appName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        znode = znode.replace(ROOT + "/", "");
        String node = watchPrevious();
        if (node.equals(znode)) {
            System.out.println("我是master");
            leader = true;
        } else {
            System.out.println("我是slave");
        }
        return true;
    }




    @Override
    public void process(WatchedEvent event) {
        // 连接状态
        Event.KeeperState keeperState = event.getState();
        // 事件类型
        Event.EventType eventType = event.getType();
        //		连接成功进来，肯定会触发
        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接上ZK服务器
            if (Event.EventType.None == eventType) {
                System.out.println("成功连接上ZK服务器");
                countDownLatch.countDown();
            }
        }
        //更新子节点
//        else if (Event.EventType.NodeChildrenChanged == eventType) {
            System.out.println("子节点变更");
            String logger = String.format("事件触发! type=%s, stat=%s, path=%s", event.getType(), event.getState(), event.getPath());
             System.out.println(logger);

            System.out.println("hello ,我的节点名是 :" + appName);
            String node = "";
            try {
//                添加watch
                node = this.watchPrevious();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (node.equals(znode)) {
                System.out.println("process: 我是 master");
                leader = true;
            } else {
                System.out.println("process: 我是 slave");
            }

//        }







    }


    private String watchPrevious() throws InterruptedException, KeeperException {
        TreeSet<String> sortedNode = new TreeSet<String>();
//        watch
        List<String> works = zk.getChildren(ROOT, this);


        for (String node : works) {
            sortedNode.add(node);
        }
        System.out.println(sortedNode.toString());

        String firstNode = sortedNode.first();

        return firstNode;

    }



    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
