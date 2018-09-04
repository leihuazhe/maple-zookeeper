package com.maple.zookeeper.maple;



public class LeaderTest {

    public static void main(String[] args) throws Exception {
        LeaderStranger work = new LeaderStranger("A1");
        work.register();
        Thread.sleep(Integer.MAX_VALUE);
    }

}
