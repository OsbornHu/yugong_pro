package com.taobao.yugong.distribute.zookeeper;

/**
 * ZKClientInfo
 *
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-09-10 14:47
 **/
public class ZKClientInfo {
    // 是否是leader 默认为false
    public static boolean isLeader = false;

    // 客户端ID
    private String id;

    // 连接信息字符串
    private String connectString;

    // 节点路径
    private String path;

    // 连接超时时间
    private Integer connectTimeOut;

    // 最大连接次数
    private Integer maxRetries;

    // 重连休眠时间
    private Integer retrySleepTime;


    public String getConnectString() {
        return connectString == null ? null : connectString.replaceAll("\\s+", "");
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(Integer connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Integer getRetrySleepTime() {
        return retrySleepTime;
    }

    public void setRetrySleepTime(Integer retrySleepTime) {
        this.retrySleepTime = retrySleepTime;
    }

    @Override
    public String toString() {
        return "ZKClientInfo{" +
                "id='" + id + '\'' +
                ", isLeader=" + isLeader +
                ", connectString='" + connectString + '\'' +
                ", path='" + path + '\'' +
                ", connectTimeOut=" + connectTimeOut +
                ", maxRetries=" + maxRetries +
                ", retrySleepTime=" + retrySleepTime +
                '}';
    }
}
