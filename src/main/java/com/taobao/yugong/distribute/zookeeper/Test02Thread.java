package com.taobao.yugong.distribute.zookeeper;

import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Test02Thread extends Thread {

    private ZKClient zkClient;

    public ZKClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZKClient zkClient) {
        this.zkClient = zkClient;
    }


    private static Log log = LogFactory.getLog(Test02Thread.class);

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

    @Override
    public void run() {
        while (true) {

            try {
                //第一步leader验证
                if(!zkClient.hasLeadership()){
                    log.info("当前服务不是leader");
                    Thread.sleep(3000);
                    continue;
                }
                else {
                    log.info("当前服务是leader");
                }

                log.info("Test02 do it... ");

            } catch (Exception e) {

            }

        }

    }

}