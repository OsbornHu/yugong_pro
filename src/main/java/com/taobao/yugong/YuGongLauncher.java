package com.taobao.yugong;

import java.io.FileInputStream;

import com.taobao.yugong.distribute.redis.PoolUtil;
import com.taobao.yugong.distribute.zookeeper.ZKClient;
import com.taobao.yugong.distribute.zookeeper.ZKClientListener;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.yugong.common.version.VersionInfo;
import com.taobao.yugong.controller.YuGongController;

public class YuGongLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(YuGongLauncher.class);

    public static void main(String[] args) throws Throwable {
        try {
            String conf = System.getProperty("yugong.conf", "classpath:yugong.properties");
            PropertiesConfiguration config = new PropertiesConfiguration();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                config.load(YuGongLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                config.load(new FileInputStream(conf));
            }

            //加入zookeeper连接代码begin
            String zkConnStr = config.getString("yugong.zookeeper.connAddr");
            logger.error("zookeeper connect string:"+zkConnStr);

            CuratorFramework client =
                    CuratorFrameworkFactory.builder()
                            .connectString(zkConnStr)
                            .retryPolicy(new ExponentialBackoffRetry(3000, 3))
                            .connectionTimeoutMs(3000)
                            .build();

            LeaderLatch leaderLatch = new LeaderLatch(client, "/leaderLatch", "client_yugong", LeaderLatch.CloseMode.NOTIFY_LEADER);
            ZKClientListener zkClientListener = new ZKClientListener();
            leaderLatch.addListener(zkClientListener);


            final ZKClient zkClient = new ZKClient(leaderLatch,client);
            try {
                zkClient.startZKClient();
            } catch (Exception e) {
                logger.error("zk客户端连接失败");
                return;
            }
            logger.info("zk客户端连接成功");
            PoolUtil.initPool(config);  //初始化redis连接池
            //加入zookeeper连接代码end

            while (true) {
                if (zkClient.hasLeadership()) {
                    logger.info("## start the YuGong.");
                    final YuGongController controller = new YuGongController(config);
                    controller.start();
                    logger.info("## the YuGong is running now ......");
                    logger.info(VersionInfo.getBuildVersion());
                    Runtime.getRuntime().addShutdownHook(new Thread() {

                        public void run() {
                            if (controller.isStart()) {
                                try {
                                    logger.info("## stop the YuGong");
                                    controller.stop();
                                    zkClient.closeZKClient();
                                } catch (Throwable e) {
                                    logger.warn("## something goes wrong when stopping YuGong:\n{}",
                                            ExceptionUtils.getFullStackTrace(e));
                                } finally {
                                    logger.info("## YuGong is down.");
                                }
                            }
                        }

                    });

                    controller.waitForDone();// 如果所有都完成，则进行退出
                    zkClient.closeZKClient();
                    Thread.sleep(3 * 1000); // 等待3s，清理上下文
                    logger.info("## stop the YuGong");
                    if (controller.isStart()) {
                        controller.stop();
                    }
                    logger.info("## YuGong is down.");
                }else{
                    Thread.sleep(500);
                }
            }
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the YuGong:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}
