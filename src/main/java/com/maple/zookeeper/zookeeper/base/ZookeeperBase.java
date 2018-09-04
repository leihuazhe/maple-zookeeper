package com.maple.zookeeper.zookeeper.base;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Zookeeper base学习笔记
 * @since 2015-6-13
 */
public class ZookeeperBase {

	/** zookeeper地址 */
	//static final String CONNECT_ADDR = "192.168.80.88:2181,192.168.80.87:2181,192.168.80.86:2181";
	static final String CONNECT_ADDR = "115.159.41.97:2181";
	/** session超时时间 5s内连接不上，超时*/
	static final int SESSION_OUTTIME = 10000;//ms
	/** 信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号 */
	static final CountDownLatch connectedSemaphore = new CountDownLatch(1);
	
	public static void main(String[] args) throws Exception {
		
		/*ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				//获取事件的状态
				KeeperState keeperState = event.getState();
				EventType eventType = event.getType();
				//如果是建立连接
				if(KeeperState.SyncConnected == keeperState){
					if(EventType.None == eventType){
						//如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
						connectedSemaphore.countDown();
						System.out.println("zk 建立连接成功");
					}
				}
			}
		});*/
		ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, (event) -> {
				//获取事件的状态
				KeeperState keeperState = event.getState();
				EventType eventType = event.getType();
				//如果是建立连接
				if(KeeperState.SyncConnected == keeperState){
					if(EventType.None == eventType){
						//如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
						connectedSemaphore.countDown();
						System.out.println("zk 建立连接成功");
					}
				}
		});

		//进行阻塞
		connectedSemaphore.await();
		
		System.out.println("--->已连上服务器，接下来开始CRUD");
		//创建父节点										认证						
		/**
		 * 有节点了，再创建 会报错
		 * OPEN_ACL_UNSAFE	完全开放
		 * CREATOR_ALL_ACL	创建该znode的连接拥有所有权限
		 *
		 * READ_ACL_UNSAFE	所有的客户端都可读
		 */
		String rel = zk.create("/testRoot", "youjieRay".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT/*_SEQUENTIAL*/);
		System.out.println("创建的节点是--->"+rel);
		//创建子节点
		String rel_chiled = zk.create("/testRoot/children", "children youjieRay".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(rel_chiled);											/**临时节点，本次会话，有效。临时节点---->>分布式锁*/
		//获取节点洗信息

		byte[] data = zk.getData("/testRoot", false, null);

		System.out.println(new String(data));

		System.out.println(zk.getChildren("/testRoot", false));
		// 获取Children值
		/**getChildren 只能get到第一层子节点*/
		List<String> list = zk.getChildren("/testRoot", false);
		for(String path :list){
			System.out.println(path);
			String realPath = "/testRoot/"+path;
			System.out.println(new String(zk.getData(realPath, false, null)));
		}
		
		//修改节点的值
		zk.setData("/testRoot", "modify data root".getBytes(), -1);
		byte[] data1 = zk.getData("/testRoot", false, null);
		System.out.println(new String(data1));
		
		//判断节点是否存在
		System.out.println(zk.exists("/testRoot/children", false));
		//删除节点
		zk.delete("/testRoot/children", -1);
		System.out.println(zk.exists("/testRoot", false));
		//异步删除节点
		/*zk.delete("/testRoot", -1, 
		
					new AsyncCallback.VoidCallback() {
						
						@Override
						public void processResult(int rc, String path, Object ctx) {
							try {
								Thread.sleep(5000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							System.out.println("--->"+rc);
							System.out.println("--->"+path);
							System.out.println("--->"+ctx);
						}
					}
		,"删除成功，回调");
		
		System.out.println("等待callback的结果");
		
		*///Thread.sleep(10000);
		
		zk.close();
		
		
		
	}
	
}
