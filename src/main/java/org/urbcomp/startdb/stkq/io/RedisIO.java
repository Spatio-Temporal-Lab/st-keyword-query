package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.manager.FilterWithHotness;
import org.urbcomp.startdb.stkq.model.BytesKey;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

public class RedisIO {
    private static final Jedis[] jedis = new Jedis[4];

    static {
        try(JedisPool pool = new JedisPool("localhost", 6379)) {
            jedis[0] = pool.getResource();
            jedis[0].select(0);
            jedis[1] = pool.getResource();
            jedis[1].select(1);
            jedis[2] = pool.getResource();
            jedis[2].select(2);
            jedis[3] = pool.getResource();
            jedis[3].select(3);
        }
    }

    public static Jedis getJedis(int db) {
        return jedis[db];
    }

    public static void putFilters(String tableName, Map<BytesKey, IFilter> filters) {
        Jedis jedis0 = jedis[0];
        System.out.println(filters.size());
//        if (jedis0.get(tableName) != null) {
//            return;
//        }
        jedis0.set(tableName, tableName);
        for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            jedis0.set(key, value);
        }
    }

    public static void putFiltersWithHotness(String tableName, Map<BytesKey, FilterWithHotness> filters) {
        Jedis jedis1 = jedis[1];
        if (jedis1.get(tableName) != null) {
            return;
        }
        jedis1.set(tableName, tableName);
        System.out.println(filters.size());
        for (Map.Entry<BytesKey, FilterWithHotness> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            jedis1.set(key, value);
        }
    }

    public static void putLRUFilters(String tableName, Map<BytesKey, IFilter> filters) {
        Jedis jedis2 = jedis[2];
        System.out.println(filters.size());
        if (jedis2.get(tableName) != null) {
            return;
        }
        System.out.println("begin to put lru filters");
        jedis2.set(tableName, tableName);
        for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            jedis2.set(key, value);
        }
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
