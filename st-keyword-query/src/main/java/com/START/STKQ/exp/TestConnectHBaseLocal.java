package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
//        HBaseUtil hBaseUtil = new HBaseUtil();
//        hBaseUtil.init("192.168.137.204");
//        System.out.println(hBaseUtil.scanAll("test03"));
        System.out.println(Bytes.compareTo(new byte[]{0, 1, 2}, new byte[]{0, 1, (byte) 255}));
        byte[] xxx = new byte[]{0, 0, (byte) 200};
        for (int i = 0; i < 100; ++i) {
            int lastByte = Bytes.incrementBytes(xxx, i)[7] & 0xFF;
            System.out.println(lastByte);
        }
        System.out.println(Bytes.toLong(new byte[]{0, 0, 0, 0, 0, 0, (byte) 255, (byte) 255}));
//        hBaseUtil.createTable("test03", "attr");

//        byte[] row = new byte[1];
//        row[0] = 1;
//        hBaseUtil.put("test03", row, "attr", "id", "1");
//        row[0] = 2;
//        hBaseUtil.put("test03", row, "attr", "id", "2");


//        hBaseUtil.createTable("test02", "attribute", BloomType.ROWPREFIX_WITH_KEYWORDS);
//        hBaseUtil.createTable("test01", "attribute");
//        for (String s : hBaseUtil.getTables()) {
//            System.out.println(s);
//        }
    }
}
