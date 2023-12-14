package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.BloomFilter;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class HBaseIO {
    private static final HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

    public static void putObjects(String tableName, ISTKeyGenerator keyGenerator, List<STObject> objects, int batchSize) throws IOException {
        List<Put> puts = new ArrayList<>();

        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            String failTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("fail time :" + failTime + " ,insert data fail,cause：" + e.getCause(0) + "，failed num：" + e.getNumExceptions());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            //重试
            String retryTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("its time to retry:" + retryTime);
            for (int i = 0; i < e.getNumExceptions(); i++) {
                org.apache.hadoop.hbase.client.Row row = null;
                try {
                    row = e.getRow(i);
                    mutator.mutate((Put) row);
                } catch (IOException ex) {
                    System.out.println("insert data fail,please check hbase status and row info : " + row);
                }
            }
        };

        BufferedMutatorParams htConfig = new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(10 * 1024 * 1024).listener(listener);

        try (BufferedMutator table = hBaseUtil.getConnection().getBufferedMutator(htConfig)) {
            for (STObject object : objects) {
                Put put = new Put(keyGenerator.toDatabaseKey(object));
//                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), Bytes.toBytes(object.getID()));
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), ByteUtil.longToBytesWithoutPrefixZero(object.getID()));
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("loc"), Bytes.toBytes(object.getLocation().toString()));
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("time"), Bytes.toBytes(DateUtil.format(object.getTime())));
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(object.getSentence()));
                puts.add(put);

                if (puts.size() >= batchSize) {
                    table.mutate(puts);
                    puts.clear();
                }
            }

            if (!puts.isEmpty()) {
                table.mutate(puts);
                puts.clear();
            }
        }
    }

    public static void putObjectsBDIA(String tableName, ISTKeyGenerator keyGenerator, List<STObject> objects, int batchSize) throws IOException {
        List<Put> puts = new ArrayList<>();
        Map<BytesKey, BloomFilter> bfs = new HashMap<>();

        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            String failTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("fail time :" + failTime + " ,insert data fail,cause：" + e.getCause(0) + "，failed num：" + e.getNumExceptions());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            //重试
            String retryTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("its time to retry:" + retryTime);
            for (int i = 0; i < e.getNumExceptions(); i++) {
                org.apache.hadoop.hbase.client.Row row = null;
                try {
                    row = e.getRow(i);
                    mutator.mutate((Put) row);
                } catch (IOException ex) {
                    System.out.println("insert data fail,please check hbase status and row info : " + row);
                }
            }
        };

        BufferedMutatorParams htConfig = new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(10 * 1024 * 1024).listener(listener);

        try (BufferedMutator table = hBaseUtil.getConnection().getBufferedMutator(htConfig)) {
            for (STObject object : objects) {
                byte[] tsCode = keyGenerator.toBytes(object);
                Put put = new Put(tsCode);
                byte[] idBytes = ByteUtil.longToBytesWithoutPrefixZero(object.getID());
                List<String> keywords = object.getKeywords();
                put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{0}, idBytes), Bytes.toBytes(object.getLocation().toString()));
                put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{1}, idBytes), Bytes.toBytes(DateUtil.format(object.getTime())));
                put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{2}, idBytes), Bytes.toBytes(String.join(" ", keywords)));

                BloomFilter bf = bfs.get(new BytesKey(tsCode));
                if (bf == null) {
                    bf = new BloomFilter(100, 10);
                    for (String s : keywords) {
                        bf.insert(s, false);
                    }
                    bfs.put(new BytesKey(tsCode), bf);
                } else {
                    for (String s : keywords) {
                        bf.insert(s, false);
                    }
                    bfs.put(new BytesKey(tsCode), bf);
                }
                puts.add(put);

                if (puts.size() >= batchSize) {
                    table.mutate(puts);
                    puts.clear();
                }
            }

            if (!puts.isEmpty()) {
                table.mutate(puts);
                puts.clear();
            }

            for (Map.Entry<BytesKey, BloomFilter> entry : bfs.entrySet()) {
                Put put = new Put(entry.getKey().getArray());
                put.addColumn(Bytes.toBytes("attr"), new byte[]{0}, entry.getValue().getArray());
                table.mutate(put);
            }
        }
    }

    public static void putFilters(String tableName, Map<BytesKey, IFilter> filters) throws IOException {
        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
                byte[] key = entry.getKey().getArray();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                entry.getValue().writeTo(bos);
                byte[] value = bos.toByteArray();

                Put put = new Put(key);
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("array"), value);
                puts.add(put);
            }
            table.put(puts);
        }
    }

    public static IFilter getFilter(String tableName, byte[] key) throws IOException {
        byte[] values = hBaseUtil.getCell(tableName, key, "attr", "array");
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return new InfiniFilter(temp.read(bis));
    }

    public static void putFilter(String tableName, byte[] key, IFilter filter) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        filter.writeTo(bos);
        byte[] value = bos.toByteArray();
        hBaseUtil.put(tableName, key, "attr", "array", value);
    }

    public static void putFilterIfNotExist(String tableName, byte[] key, IFilter filter) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        filter.writeTo(bos);
        byte[] value = bos.toByteArray();
        hBaseUtil.putIfNotExist(tableName, key, "attr", "array", value);
    }

    // write some unused data
    public void putUnusedData(String tableName, int rubbishLength, int size) throws IOException {
        List<Put> puts = new ArrayList<>();
        byte[] rubbish = new byte[rubbishLength];
        int n = rubbish.length;
        for (int i = 0; i < n; ++i) {
            rubbish[i] = -1;
        }
        for (int i = 0; i < size; ++i) {
            Put put = new Put(ByteUtil.concat(rubbish, ByteUtil.longToBytes(i)));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("loc"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("time"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(i));
            puts.add(put);
        }
        Table table = hBaseUtil.getTable(tableName);
        table.put(puts);
    }
}
