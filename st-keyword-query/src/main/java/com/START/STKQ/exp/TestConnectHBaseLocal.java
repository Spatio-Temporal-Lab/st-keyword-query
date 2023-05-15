package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");
        hBaseUtil.createTable("test02", "attribute", BloomType.ROWPREFIX_WITH_KEYWORDS);
//        hBaseUtil.createTable("test01", "attribute");
        for (String s : hBaseUtil.getTables()) {
            System.out.println(s);
        }
    }
}
