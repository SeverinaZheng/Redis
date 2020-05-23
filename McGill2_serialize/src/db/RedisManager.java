package db;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
 
public class RedisManager {
    private static final RedisManager instance = new RedisManager();
    private static JedisPool pool;
    private RedisManager() {}
    public final static RedisManager getInstance() {
        return instance;
    }
    public void connect() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(60);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestWhileIdle(true);
        pool = new JedisPool(poolConfig, "127.0.0.1", 6379);
    }
    public void release() {
        pool.destroy();
    }
    public Jedis getJedis() {
        return pool.getResource();
    }
    public void returnJedis(Jedis jedis) {
        pool.returnResource(jedis);
    }
}