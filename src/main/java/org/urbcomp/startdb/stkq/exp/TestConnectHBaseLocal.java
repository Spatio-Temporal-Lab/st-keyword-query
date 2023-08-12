package org.urbcomp.startdb.stkq.exp;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");

        //109306
        String tableName = "test01";

//        int maxKeys = 100_0000;
//
//        hBaseUtil.truncateTable(tableName);
//        List<Put> puts = new ArrayList<>();
//        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {
//
//            long st = 0;
//            long id = 0;
//            for (int i = 0; i < 10; ++i) {
//                for (int j = 0; j < maxKeys; ++j) {
//                    Put put = new Put(ByteUtil.concat(ByteUtil.getKByte(st, 7), ByteUtil.longToByte(++id)));
//                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(String.valueOf(st)));
//
//                    puts.add(put);
//                    if (puts.size() >= 5000) {
//                        table.put(puts);
//                        puts.clear();
//                    }
//                }
//                ++st;
//            }
//
//            if (!puts.isEmpty()) {
//                table.put(puts);
//                puts.clear();
//            }
//        }
//
//        hBaseUtil.flushTable(tableName);

        long start = System.currentTimeMillis();
        boolean useBfInHBase = true;
        for (int st = 0; st < 9; ++st) {
            String[] keywords = new String[1];
            keywords[0] = String.valueOf(st + 1);
            System.out.println(
                    hBaseUtil.scanWithKeywords(tableName, useBfInHBase, keywords,
                            ByteUtil.concat(ByteUtil.getKByte((long) st, 7), new byte[]{0, 0, 0, 0, 0, 0, 0, 0}),
                            ByteUtil.concat(ByteUtil.getKByte((long) st + 1, 7), ByteUtil.longToByte(Long.MAX_VALUE)),
                            QueryType.CONTAIN_ONE
                    ).size()
            );
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}
