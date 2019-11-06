package com.taobao.yugong.positioner;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.distribute.redis.JedisUtil;
import com.taobao.yugong.exception.YuGongException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;

import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * 使用redis 持久化最后处理的位置
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-11-06 09:46
 **/
public class RedisMixedRecordPositioner extends MemoryRecordPositioner implements RecordPositioner {
    private static final Logger logger       = LoggerFactory.getLogger(FileMixedRecordPositioner.class);
    private static final Charset charset      = Charset.forName("UTF-8");

    private ScheduledExecutorService executor;
    private static final String cacheKeyPre = "yugong.position.key.";
    private  String cacheKey = "";
    private long                     period       = 100;// 单位ms

    private Pool pool;

    private AtomicBoolean needFlush    = new AtomicBoolean(false);
    private AtomicBoolean            needReload   = new AtomicBoolean(true);


    public void start() {
        super.start();

        executor = Executors.newScheduledThreadPool(1);
        // 启动定时工作任务
        executor.scheduleAtFixedRate(new Runnable() {

            public void run() {
                try {
                    JedisUtil jedisUtil = new JedisUtil();
                    jedisUtil.setPool(pool);
                    // 定时将内存中的最新值刷到file中，多次变更只刷一次
                    if (needFlush.compareAndSet(true, false)) {
                        Position position = getLatest(jedisUtil);
                        flushDataToCache(jedisUtil, position);
                    }
                } catch (Throwable e) {
                    // ignore
                    logger.error("period update position failed!", e);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        super.stop();
        JedisUtil jedisUtil = new JedisUtil();
        jedisUtil.setPool(pool);
        flushDataToCache(jedisUtil, super.getLatest());
        executor.shutdownNow();
    }

    public void persist(Position position) {
        needFlush.set(true);
        super.persist(position);
    }

    public Position getLatest(JedisUtil jedisUtil) {
        if (needReload.compareAndSet(true, false)) {
            Position position = loadDataFromCache(jedisUtil);
            super.persist(position);
            return position;
        } else {
            return super.getLatest();
        }
    }

    // ============================ helper method ======================

    private void flushDataToCache(JedisUtil jedisUtil, Position position) {
        if (position != null) {
            String json = JSON.toJSONString(position,
                    SerializerFeature.WriteClassName,
                    SerializerFeature.WriteNullListAsEmpty);
            try {
                jedisUtil.set(cacheKey,json);
            } catch (Exception e) {
                throw new YuGongException(e);
            }
        }
    }

    private Position loadDataFromCache(JedisUtil jedisUtil) {
        try {
            if (!jedisUtil.exists(cacheKey)) {
                return null;
            }

            String json = jedisUtil.get(cacheKey);
            return JSON.parseObject(json, Position.class);
        } catch (Exception e) {
            throw new YuGongException(e);
        }
    }


    public void setCacheKey(String cacheKey){
        this.cacheKey = cacheKey;
    }

    public String getCacheKeyPre(){
        return cacheKeyPre;
    }

    public void setPool(Pool pool){
        this.pool = pool;
    }

}
