package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class RedisIO {
    private static final Jedis jedis;

    static {
        try(JedisPool pool = new JedisPool("localhost", 6379)) {
            jedis = pool.getResource();
        }
    }

    public static void putFilters(String tableName, Map<BytesKey, IFilter> filters) {
        if (jedis.get(tableName) != null) {
            return;
        }
        jedis.set(tableName, tableName);
        System.out.println(filters.size());
        for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
            byte[] key = entry.getKey().getArray();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            entry.getValue().writeTo(bos);
            byte[] value = bos.toByteArray();
            jedis.set(key, value);
        }
    }

    public static IFilter getFilter(byte[] key) {
        byte[] values = jedis.get(key);
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return new InfiniFilter(temp.read(bis));
    }
}
