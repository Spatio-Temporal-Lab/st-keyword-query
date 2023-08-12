package org.urbcomp.startdb.stkq.exp;

import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;

public class TestCompact {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");
        String tableName = "test01";
        hBaseUtil.deleteTable(tableName);
        hBaseUtil.createTable(tableName, "attr", BloomType.ROWPREFIX_FIXED_LENGTH);

        hBaseUtil.put(tableName, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                "attr", "keywords", "a b");
        hBaseUtil.flushTable(tableName);

        hBaseUtil.put(tableName, new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
                "attr", "keywords", "b c");
        hBaseUtil.flushTable(tableName);
        hBaseUtil.put("test01", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
                "attr", "keywords", "c d");
        hBaseUtil.put("test01", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},
                "attr", "keywords", "d e");
        Admin admin = hBaseUtil.getConnection().getAdmin();
        admin.majorCompact(TableName.valueOf("test01"));

//        System.out.println(hBaseUtil.scanWithKeywords("test01", new String[]{"a"},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                ByteUtil.concat(ByteUtil.getKByte(0L, 7), ByteUtil.longToByte(Long.MAX_VALUE)),
//                QueryType.CONTAIN_ONE));
    }
}
