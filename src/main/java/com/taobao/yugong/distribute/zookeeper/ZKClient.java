package com.taobao.yugong.distribute.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
/**
 * ZKClient
 *
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-09-10 14:47
 **/
public class ZKClient {
    private LeaderLatch leader;

    private CuratorFramework client;

    public ZKClient (LeaderLatch leader,CuratorFramework client){
        this.client = client;
        this.leader = leader;
    }

    /**
     * 启动客户端
     * @throws Exception
     */
    public void startZKClient() throws Exception {
        client.start();
        leader.start();
    }

    /**
     * 关闭客户端
     * @throws Exception
     */
    public void closeZKClient() throws Exception {
        leader.close();
        client.close();
    }

    /**
     * 判断是否变为领导者
     * @return
     */
    public boolean hasLeadership(){
        return leader.hasLeadership() && ZKClientInfo.isLeader;
    }


    public LeaderLatch getLeader() {
        return leader;
    }

    public void setLeader(LeaderLatch leader) {
        this.leader = leader;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }


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
        zkClient.startZKClient();
        Thread.sleep(5000);

        int i = 0;
        while (i<15){
            //System.out.println("hasLeadership = "+zkClient.hasLeadership());
            Thread.sleep(1000);
            i++;
        }
        zkClient.closeZKClient();
        Thread.sleep(5000);
    }
}
