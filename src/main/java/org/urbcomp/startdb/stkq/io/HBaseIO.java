package org.urbcomp.startdb.stkq.io;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), Bytes.toBytes(object.getID()));
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

    public static void putFilters(String tableName, Map<BytesKey, IFilter> filters) throws IOException {

        System.out.println("begin put filters");
        if (hBaseUtil.existsTable(tableName)) {
            return;
        } else {
            hBaseUtil.createTable(tableName, new String[]{"attr"});
        }

        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {
            System.out.println(filters.size());
            for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
                byte[] key = entry.getKey().getArray();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                entry.getValue().writeTo(bos);
                byte[] value = bos.toByteArray();

                Put put = new Put(key);
                put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("array"), value);
                table.put(put);
            }
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

    // write some unused data
    public void putUnusedData(String tableName, int rubbishLength, int size) throws IOException {
        List<Put> puts = new ArrayList<>();
        byte[] rubbish = new byte[rubbishLength];
        int n = rubbish.length;
        for (int i = 0; i < n; ++i) {
            rubbish[i] = -1;
        }
        for (int i = 0; i < size; ++i) {
            Put put = new Put(ByteUtil.concat(rubbish, ByteUtil.longToByte(i)));
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
