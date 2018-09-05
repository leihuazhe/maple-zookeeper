package com.maple.zookeeper.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author maple 2018.09.05 上午9:26
 */
public class ZookeeperClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchTest.class);
    private static final String CONNECT_ADDR = "127.0.0.1:2181";
    private static final String CONFIG_PATH = "/test";
    private static final String RUNTIME_PATH = "/runtime";
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

    private static ZookeeperClient zookeeperClient;
    /**
     *
     * 单例
     */
    public static ZookeeperClient getInstance(){
        if(zookeeperClient==null){
            synchronized (ZookeeperClient.class){
                if (zookeeperClient==null){
                    zookeeperClient = new ZookeeperClient();
                }
            }
        }
        return zookeeperClient;
    }


    private ZookeeperClient() {
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
                    LOGGER.info("zk 建立连接成功");
                }
            }
        });
        //进行阻塞
        CONNECTEDSEMAPHORE.await();
        LOGGER.info("--->已连上服务器，接下来开始CRUD");
    }


    public void syncService(ZkServiceInfo zkInfo) throws KeeperException, InterruptedException {
        syncZkRuntimeInfo(zkInfo);
        syncZkConfigInfo(zkInfo);

    }

    public void syncZkRuntimeInfo(ZkServiceInfo zkInfo) throws KeeperException, InterruptedException {
        //1.获取 globalConfig  异步模式
        List<String> children = zookeeper.getChildren(RUNTIME_PATH, zkInfo.getWatcher());
        LOGGER.info("syncZkRuntimeInfo children,{}, {}", children, zkInfo.counter);
    }


    public void syncZkConfigInfo(ZkServiceInfo zkInfo) {
        //1.获取 globalConfig  异步模式
        zookeeper.getData(CONFIG_PATH, zkInfo.getWatcher(), globalConfigDataCb, zkInfo);
    }


    /**
     * 全局配置异步getData
     */
    private AsyncCallback.DataCallback globalConfigDataCb = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                LOGGER.error("读取配置节点data时连接丢失，重新获取!");
                syncZkConfigInfo((ZkServiceInfo) ctx);
                break;
            case NONODE:
                LOGGER.error("全局配置节点不存在");
                break;
            case OK:
                LOGGER.info("------> 异步获取zk data,获取次数:{}", ((ZkServiceInfo) ctx).configCounter);
                break;
            default:
                break;
        }
    };

}
