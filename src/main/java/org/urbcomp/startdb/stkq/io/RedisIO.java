package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.Range;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisIO {
    private static final int CONNECT_COUNT = 4;
    private static final Jedis[] jedis = new Jedis[CONNECT_COUNT];
//    private static final String ip = "10.242.6.16";
    private static final String ip = "localhost";

    static {
        try(JedisPool pool = new JedisPool(ip, 6379)) {
            for (int i = 0; i < CONNECT_COUNT; ++i) {
                jedis[i] = pool.getResource();
                jedis[i].select(i);
            }
        }
    }

    public static void putFilters(int db, Map<BytesKey, IFilter> filters) {
        Jedis con = jedis[db];
        Pipeline pipeline = new Pipeline(con);
        for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            pipeline.set(key, value);
//            con.set(key, value);
        }
        pipeline.sync();
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

    public static List<byte[]> get(int db, List<byte[]> keyList) {
        Jedis con = jedis[db];
        Pipeline pipeline = con.pipelined();

        List<Response<byte[]>> responses = new ArrayList<>();
        for (byte[] key : keyList) {
            responses.add(pipeline.get(key));
        }
        pipeline.sync();
        List<byte[]> result = new ArrayList<>();
        for (Response<byte[]> response : responses) {
            result.add(response.get());
        }
        return result;
    }

    public static void close() {
        for (Jedis j : jedis) {
            j.close();
        }
    }

    public static void zAdd(int db, byte[] key, long score, byte[] value) {
        Jedis con = jedis[db];
        con.zadd(key, score, value);
    }

    public static List<byte[]> rangeQuery(int db, byte[] key, long min, long max) {
        Jedis con = jedis[db];
        return con.zrangeByScore(key, min, max);
    }

    public static List<byte[]> rangeQueries(int db, byte[] key, List<Range<Long>> ranges) {
        Jedis con = jedis[db];
        Pipeline pipeline = con.pipelined();

        List<Response<List<byte[]>>> responses = new ArrayList<>();
        for (Range<Long> range : ranges) {
            responses.add(pipeline.zrangeByScore(key, range.getLow(), range.getHigh()));
        }
        pipeline.sync();
        List<byte[]> result = new ArrayList<>();
        for (Response<List<byte[]>> response : responses) {
            result.addAll(response.get());
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(jedis[0].info());
        System.out.println(jedis[1].info());
    }

    public static ChainedInfiniFilter getFilterInChainType(int db, byte[] key) {
        Jedis con = jedis[db];
        byte[] values = con.get(key);
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return temp.read(bis);
    }

    public static void flush() {
        for (int i = 0; i < CONNECT_COUNT; ++i) {
            jedis[i].flushDB();
        }
    }
}
