package com.maple.zookeeper.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author maple 2018.09.04 上午9:41
 */
public class WatchTest {
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
                    LOGGER.info("zk 建立连接成功");
                }
            }
        });
        //进行阻塞
        CONNECTEDSEMAPHORE.await();
        LOGGER.info("--->已连上服务器，接下来开始CRUD");
    }

    protected void syncService(ZkInfo zkInfo) throws KeeperException, InterruptedException {
        syncZkRuntimeInfo(zkInfo);
        syncZkConfigInfo(zkInfo);

    }

    private void syncZkRuntimeInfo(ZkInfo zkInfo) throws KeeperException, InterruptedException {
        //1.获取 globalConfig  异步模式
        List<String> children = zookeeper.getChildren(RUNTIME_PATH, new ZKWathcer(zkInfo, type -> {
            if (type == 1) {
                try {
                    syncZkRuntimeInfo(zkInfo);
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }));
        LOGGER.info("syncZkRuntimeInfo children,{}, {}", children, zkInfo.counter);
    }


    private void syncZkConfigInfo(ZkInfo zkInfo) {
        //1.获取 globalConfig  异步模式
        zookeeper.getData(CONFIG_PATH, new ZKWathcer(zkInfo, type -> {
            if (type == 2) {
                syncZkConfigInfo(zkInfo);
            }
        }), globalConfigDataCb, zkInfo);
    }


    /**
     * 全局配置异步getData
     */
    private AsyncCallback.DataCallback globalConfigDataCb = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                LOGGER.error("读取配置节点data时连接丢失，重新获取!");
                syncZkConfigInfo((ZkInfo) ctx);
                break;
            case NONODE:
                LOGGER.error("全局配置节点不存在");
                break;
            case OK:
                LOGGER.info("------> 异步获取zk data :{}", ((ZkInfo) ctx).configCounter);
                break;
            default:
                break;
        }
    };


    public static void main(String[] args) throws InterruptedException, KeeperException {
        CountDownLatch cdl = new CountDownLatch(1);
        WatchTest watchTest = new WatchTest();
        for (int i = 0; i < 1000; i++) {
            watchTest.syncService(new ZkInfo("zookeeper watch " + i));
        }

        cdl.await();
    }

    static class ZkInfo {
        final String data;
        int counter = 1;
        int configCounter = 1;

        ZkInfo(String data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return data + " counter:" + counter;
        }
    }

    interface Callback {

        void callback(int type);
    }


    static class ZKWathcer implements Watcher {
        final ZkInfo zkInfo;
        final Callback callback;


        ZKWathcer(ZkInfo zkInfo, Callback callback) {
            this.zkInfo = zkInfo;
            this.callback = callback;
        }


        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                if (zkInfo.counter++ < 10) {
                    LOGGER.info(getClass().getSimpleName() + "<--> {} 子节点发生变化，重新获取配置信息", watchedEvent.getPath());
                    callback.callback(1);
//                        syncZkRuntimeInfo(zkInfo);
                }
            } else if (watchedEvent.getType() == Watcher.Event.EventType.NodeDataChanged) {
                if (zkInfo.configCounter++ < 10) {
                    LOGGER.info(getClass().getSimpleName() + "<--> {} 节点内容发生变化，重新获取配置信息", watchedEvent.getPath());
                    callback.callback(2);
                }
            }


        }
    }

}
