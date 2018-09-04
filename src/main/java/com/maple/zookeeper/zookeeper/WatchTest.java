package com.maple.zookeeper.zookeeper;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author maple 2018.09.04 上午9:41
 */
public class WatchTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchTest.class);
    private static final String CONNECT_ADDR = "127.0.0.1:2181";
    private static final String CONFIG_PATH = "/test";
    private static final String NODE_PATH = "/node";
    /**
     * session超时时间 10s 内连接不上，超时
     */
    private static final int SESSION_OUTTIME = 10000;
    /**
     * 信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号
     */
    private static final CountDownLatch CONNECTEDSEMAPHORE = new CountDownLatch(1);
    private ZooKeeper zookeeper;


    public WatchTest() {
        try {
            init();
        } catch (InterruptedException | IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * init()
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void init() throws InterruptedException, IOException {
        zookeeper = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, (event) -> {
            //获取事件的状态
            Watcher.Event.KeeperState keeperState = event.getState();
            Watcher.Event.EventType eventType = event.getType();
            //如果是建立连接
            if (Watcher.Event.KeeperState.SyncConnected == keeperState) {
                if (Watcher.Event.EventType.None == eventType) {
                    //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                    CONNECTEDSEMAPHORE.countDown();
                    System.out.println("zk 建立连接成功");
                }
            }
        });
        //进行阻塞
        CONNECTEDSEMAPHORE.await();
        System.out.println("--->已连上服务器，接下来开始CRUD");
    }

    protected void syncZkConfigInfo(String info) {
        System.out.println("into sync config info ");
        //1.获取 globalConfig  异步模式
        zookeeper.getData(CONFIG_PATH, watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                System.out.println(getClass().getSimpleName() + "<--> {" + watchedEvent.getPath() + "} 节点内容发生变化，重新获取配置信息");
                syncZkConfigInfo(info);
            }
        }, globalConfigDataCb, info);
    }

    /**
     * 全局配置异步getData
     */
    private AsyncCallback.DataCallback globalConfigDataCb = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                System.out.println("读取配置节点data时连接丢失，重新获取!");
                syncZkConfigInfo((String) ctx);
                break;
            case NONODE:
                System.out.println("全局配置节点不存在");
                break;
            case OK:
                System.out.println("------> 异步获取zk data : " + new String(data));
                break;
            default:
                break;
        }
    };


    public static void main(String[] args) throws InterruptedException {
        WatchTest watchTest = new WatchTest();
        watchTest.syncZkConfigInfo("zookeeper watch ");
        Thread.sleep(Long.MAX_VALUE);
    }


}
