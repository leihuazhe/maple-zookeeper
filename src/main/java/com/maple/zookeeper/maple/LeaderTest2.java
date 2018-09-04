package com.maple.zookeeper.maple;



public class LeaderTest2 {

    public static void main(String[] args) throws Exception {
        LeaderStranger work = new LeaderStranger("A3");
        work.register();
        Thread.sleep(Integer.MAX_VALUE);
    }

}
