package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;

import java.io.IOException;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");


//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "attr", "keywords", "a b c");
//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, "attr", "keywords", "c d");

//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, "attr", "keywords", "e f");
//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}, "attr", "keywords", "g h");
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"a", "e"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
                null
        ));
//        System.out.println(hBaseUtil.scanAll(
//                "testKeywords"
//        ));
//        System.out.println(hBaseUtil.scan(
//                "testKeywords",
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
//        ));
//        hBaseUtil.createTable("test01", "attribute");
    }
}
