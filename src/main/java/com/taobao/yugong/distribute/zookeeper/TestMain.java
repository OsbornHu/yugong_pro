package com.taobao.yugong.distribute.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class TestMain {

    private static Log log = LogFactory.getLog(TestMain.class);

    public static void main(String[] args) throws Exception {
        CuratorFramework client =
                CuratorFrameworkFactory.builder()
                        .connectString("10.108.1.57:2181")
                        .retryPolicy(new ExponentialBackoffRetry(5000, 3))
                        .connectionTimeoutMs(5000)
                        .build();

        LeaderLatch leaderLatch = new LeaderLatch(client, "/leaderLatch", "client1", LeaderLatch.CloseMode.NOTIFY_LEADER);
        ZKClientListener zkClientListener = new ZKClientListener();
        leaderLatch.addListener(zkClientListener);


        ZKClient zkClient = new ZKClient(leaderLatch,client);
        try {
            zkClient.startZKClient();
        } catch (Exception e) {
            log.error("zk客户端连接失败");
            return;
        }
        log.info("zk客户端连接成功");

        Test01Thread test01Thread = new Test01Thread();
        test01Thread.setZkClient(zkClient);
        test01Thread.start();

        Test02Thread test02Thread = new Test02Thread();
        test02Thread.setZkClient(zkClient);
        test02Thread.start();

    }
}
