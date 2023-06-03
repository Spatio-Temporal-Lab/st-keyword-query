package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;
import com.START.STKQ.util.ByteUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");

        //109306
        String tableName = "test01";

//        int maxKeys = 109306;
//        int now = 0;
//        hBaseUtil.truncateTable(tableName);
//        List<Put> puts = new ArrayList<>();
//        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {
//            for (int i = 0; i < 2; ++i) {
//                for (int j = 0; j < maxKeys; ++j) {
//
//                    int st = now / 10_0000;
//                    ++now;
//
//                    Put put = new Put(ByteUtil.concat(ByteUtil.getKByte((long) st, 7), ByteUtil.longToByte(now)));
//                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(String.valueOf(now)));
//                    puts.add(put);
//                    if (puts.size() >= 5000) {
//                        table.put(puts);
//                        puts.clear();
//                    }
//                }
//            }
//            if (!puts.isEmpty()) {
//                table.put(puts);
//                puts.clear();
//            }
//        }
//        hBaseUtil.flushTable(tableName);

        boolean useBfInHBase = true;

        System.out.println(hBaseUtil.scanAll(tableName).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                tableName,
                useBfInHBase, new String[]{"a"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                tableName,
                useBfInHBase, new String[]{"b"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                tableName,
                useBfInHBase, new String[]{"c"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                tableName,
                useBfInHBase, new String[]{"d"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                tableName,
                useBfInHBase, new String[]{"e"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
    }
}
