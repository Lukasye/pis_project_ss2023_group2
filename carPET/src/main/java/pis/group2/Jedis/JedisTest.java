package pis.group2.Jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class JedisTest {
    public static void main(String[] args) {
//        Jedis jedis = new Jedis("redis://default:123456@redis-17361.c81.us-east-1-2.ec2.cloud.redislabs.com:17361");
//        String locationPET = jedis.get("LocationPET");
//        System.out.println(locationPET);
//        jedis.close();

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxWait(Duration.ofMillis(200));
        JedisPool asd = new JedisPool(jedisPoolConfig, "redis://default:123456@redis-17361.c81.us-east-1-2.ec2.cloud.redislabs.com", 17361, 1000);
        Jedis jedis = asd.getResource();
        String locationPET = jedis.get("LocationPET");
        System.out.println(locationPET);
        jedis.close();
        asd.destroy();

    }
}
