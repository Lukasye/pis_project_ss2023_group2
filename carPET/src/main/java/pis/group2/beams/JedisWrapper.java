package pis.group2.beams;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class JedisWrapper implements Serializable {
    private final Jedis jedis;

    public JedisWrapper(Jedis jedis) {
        this.jedis = jedis;
    }

    public void tearDown(){
        this.jedis.close();
    }

    public Integer get(String policy){
        return Integer.valueOf(jedis.get(policy));
    }

    public void set(String key, Integer value){
        jedis.set(key, String.valueOf(value));
    }
}
