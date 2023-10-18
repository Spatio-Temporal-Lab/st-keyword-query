package org.urbcomp.startdb.stkq.io;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

public class RedisIOTest {
    @Test
    public void testSet() {
        Jedis jedis = RedisIO.getJedis(0);
        byte[] key = new byte[]{0};
        byte[] value = new byte[]{0};
        jedis.set(key, value);
        System.out.println(Arrays.toString(jedis.get(key)));
        Assert.assertArrayEquals(jedis.get(key), value);
        value[0] = 1;
        jedis.set(key, value);
        System.out.println(Arrays.toString(jedis.get(key)));
        Assert.assertArrayEquals(jedis.get(key), value);
    }
}
