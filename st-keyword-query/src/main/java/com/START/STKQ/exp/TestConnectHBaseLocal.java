package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");
        System.out.println(hBaseUtil.scanAll("test03"));
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
