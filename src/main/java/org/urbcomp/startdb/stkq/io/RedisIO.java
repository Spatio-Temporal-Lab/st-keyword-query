package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class RedisIO {
    private static final int CONNECT_COUNT = 4;
    private static final Jedis[] jedis = new Jedis[CONNECT_COUNT];

    static {
        try(JedisPool pool = new JedisPool("localhost", 6379)) {
            for (int i = 0; i < CONNECT_COUNT; ++i) {
                jedis[i] = pool.getResource();
                jedis[i].select(i);
            }
        }
    }

    public static void putFilters(int db, Map<BytesKey, IFilter> filters) {
        Jedis jedis0 = jedis[db];
        for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            jedis0.set(key, value);
        }
    }

    public static void putFilter(int db, byte[] key, IFilter filter) {
        Jedis con = jedis[db];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        filter.writeTo(bos);
        byte[] value = bos.toByteArray();
        con.set(key, value);
    }

    public static IFilter getFilter(int db, byte[] key) {
        byte[] values = jedis[db].get(key);
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return new InfiniFilter(temp.read(bis));
    }

    public static IFilter getFilter(byte[] key) {
        return getFilter(0, key);
    }

    public static void set(int db, byte[] key, byte[] value) {
        Jedis con = jedis[db];
        con.set(key, value);
    }

    public static byte[] get(int db, byte[] key) {
        Jedis con = jedis[db];
        return con.get(key);
    }

    public static void close() {
        for (Jedis j : jedis) {
            j.close();
        }
    }

    public static void main(String[] args) {
        System.out.println(jedis[0].info());
        System.out.println(jedis[1].info());
    }
}
