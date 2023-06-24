package pis.group2.Jedis;

import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import pis.group2.beams.JedisWrapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.time.Duration;

public class DataFetcher implements Serializable {
    private JedisPool jedisPool;

    public DataFetcher() {
    }

    public DataFetcher(String ip, String pass) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxWait(Duration.ofMillis(200));
        jedisPool= new JedisPool(jedisPoolConfig, ip, 6379, 1000, pass);
    }

    public static JedisPool jedisPoolFactory(Tuple3<String, String, String> config){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxWait(Duration.ofMillis(200));
        return new JedisPool(jedisPoolConfig, config.f0, 6379, 1000, config.f1);
    }

    public JedisWrapper getJedis(){
        return new JedisWrapper(this.jedisPool.getResource());
    }

    public static void tearDown(JedisWrapper jedis){
        if (jedis != null){
            jedis.tearDown();
        }
    }

    public static void main(String[] args) {
        DataFetcher dataFetcher = new DataFetcher("192.168.1.13", "123456");
        JedisWrapper jedis = dataFetcher.getJedis();
        Integer speedSituation = jedis.get("SpeedSituation");
        System.out.println(speedSituation);
        jedis.set("SpeedSituation", 6);
        speedSituation = jedis.get("SpeedSituation");
        System.out.println(speedSituation);

    }

}
