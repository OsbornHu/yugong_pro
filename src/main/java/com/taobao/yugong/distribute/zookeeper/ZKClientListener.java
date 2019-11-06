package com.taobao.yugong.distribute.zookeeper;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * ZKClientListener
 *
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-09-10 14:48
 **/
public class ZKClientListener implements LeaderLatchListener {

    private static Log log = LogFactory.getLog(ZKClientListener.class);

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

    @Override
    public void isLeader() {
        log.info( simpleDateFormat.format(new Date()) + "当前服务已变为leader，服务启动");
        ZKClientInfo.isLeader = true;

    }

    @Override
    public void notLeader() {
        log.info( simpleDateFormat.format(new Date()) + "当前服务已退出leader，服务停止");
        ZKClientInfo.isLeader = false;

    }

}
