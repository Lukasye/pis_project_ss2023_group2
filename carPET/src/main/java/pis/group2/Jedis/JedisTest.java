package pis.group2.Jedis;

import redis.clients.jedis.Jedis;

public class JedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("redis://default:123456@redis-17361.c81.us-east-1-2.ec2.cloud.redislabs.com:17361");
        String locationPET = jedis.get("LocationPET");
        System.out.println(locationPET);
        jedis.close();
    }
}
