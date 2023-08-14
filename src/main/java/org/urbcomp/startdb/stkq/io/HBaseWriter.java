package org.urbcomp.startdb.stkq.io;

import org.urbcomp.startdb.stkq.keyGenerator.AbstractSTKeyGenerator;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter {
    private final HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

    private final AbstractSTKeyGenerator keyGenerator;

    public HBaseWriter(AbstractSTKeyGenerator keyGenerator) {
        this.keyGenerator = keyGenerator;
    }

    public void putObjects(String tableName, ArrayList<STObject> objects, int batchSize) throws IOException {
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
                Put put = new Put(ByteUtil.toByte(keyGenerator.toKey(object)));
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
