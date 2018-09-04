package com.maple.zookeeper.maple;



public class LeaderTest1 {

    public static void main(String[] args) throws Exception {
        LeaderStranger work = new LeaderStranger("A2");
        work.register();
        Thread.sleep(Integer.MAX_VALUE);
    }

}
