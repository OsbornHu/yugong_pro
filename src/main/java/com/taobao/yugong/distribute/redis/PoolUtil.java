package com.taobao.yugong.distribute.redis;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

import java.util.HashSet;
import java.util.Set;

/**
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-11-06 11:08
 **/
public class PoolUtil {

    public  static Pool pool = null;


    public static void initPool(PropertiesConfiguration config) {
        String poolRedisType = config.getString("yugong.redis.pool.type");
        String mode = config.getString("yugong.run.positioner");
        if(StringUtils.isEmpty(mode)){  //yugong.run.positioner未设置值直接退出
            return ;
        }else{
           if(!mode.equalsIgnoreCase("REDIS")) { //yugong.run.positioner值未设为REDIS直接退出
               return ;
           }
        }
        if(StringUtils.isEmpty(poolRedisType)){ //yugong.redis.pool.type未设置值，直接退出
            return ;
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        int maxTotal = config.getInt("yugong.redis.pool.maxtotal",50);
        jedisPoolConfig.setMaxTotal(maxTotal);
        int maxIdle = config.getInt("yugong.redis.pool.maxidle",20);
        jedisPoolConfig.setMaxIdle(maxIdle);
        int minIdle = config.getInt("yugong.redis.pool.minidle",10);
        jedisPoolConfig.setMinIdle(minIdle);
        int timeout = config.getInt("yugong.redis.pool.timeout",3000);
        jedisPoolConfig.setBlockWhenExhausted(true);//连接耗尽的时候，是否阻塞，false会抛出异常，true阻塞直到超时。默认为true。

        String addr = config.getString("yugong.redis.pool.addr");  //连接IP和端口，sentinel模式下用逗号隔开多个
        if(poolRedisType.equalsIgnoreCase("alone")){
            String[] ipports = StringUtils.split(addr,":");
            String redisIp = ipports[0];
            int redisPort = Integer.valueOf(ipports[1]);
            pool = new JedisPool(jedisPoolConfig,redisIp,redisPort,timeout);
        }else if(poolRedisType.equalsIgnoreCase("sentinel")){
            Set<String> set = new HashSet<String>();
            String masterName = config.getString("yugong.redis.pool.master");
            String[] addrArr = StringUtils.split(addr,",");
            for(String ipport:addrArr){
                set.add(ipport);
            }
            pool = new JedisSentinelPool(masterName, set, jedisPoolConfig, timeout);
        }

    }

    public static Pool getPool(){
        if(pool==null){
            throw new NullPointerException("未初如化redis连接池");
        }
        return pool;
    }


}
