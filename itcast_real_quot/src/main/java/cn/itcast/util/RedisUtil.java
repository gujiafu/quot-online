package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;


public class RedisUtil {

    /**
     * 开发步骤：
     * 1.新建获取连接的方法
     * 2.初始化连接池
     * 3.设计连接集群地址
     * 4.获取客户端连接对象
     */
    public static JedisCluster getJedisCluster(){

        JedisPoolConfig config = new JedisPoolConfig();
        String host = QuotConfig.config.getProperty("redis.host");
        String maxTotal = QuotConfig.config.getProperty("redis.maxTotal");
        String minIdle = QuotConfig.config.getProperty("redis.minIdle");
        String maxIdle = QuotConfig.config.getProperty("redis.maxIdle");

        config.setMaxTotal(Integer.parseInt(maxTotal));
        config.setMinIdle(Integer.parseInt(minIdle));
        config.setMaxIdle(Integer.parseInt(maxIdle));

        HashSet<HostAndPort> set = new HashSet<>();
        String[] arr = host.split(",");
        for (String line : arr) {
            String[] strArr = line.split(":");
            set.add(new HostAndPort(strArr[0],Integer.parseInt(strArr[1])));
        }

        JedisCluster jedisCluster = new JedisCluster(set, config);
        return jedisCluster;
    }

    public static void main(String[] args) throws IOException {
        //redis客户端集群链接对象
        JedisCluster jedisCluster = getJedisCluster();
//        jedisCluster.hset("quot", "zf", "-1") ;//振幅
//        jedisCluster.hset("quot", "upDown1", "-1"); //涨跌幅-跌幅
//        jedisCluster.hset("quot", "upDown2", "100") ;//涨跌幅-涨幅
//        jedisCluster.hset("quot", "hsl", "-1") ;//换手率
//        jedisCluster.close();
        String str = jedisCluster.hget("product", "apple");
        System.out.println("<<<<:"+str);

    }

}
