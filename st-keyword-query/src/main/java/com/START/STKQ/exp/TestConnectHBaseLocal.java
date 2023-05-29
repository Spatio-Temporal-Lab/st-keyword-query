package com.START.STKQ.exp;

import com.START.STKQ.io.HBaseUtil;
import com.START.STKQ.util.ByteUtil;

import java.io.IOException;
import java.util.Arrays;

public class TestConnectHBaseLocal {
    public static void main(String[] args) throws IOException {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");


//        hBaseUtil.put("test01", new byte[]{0}, "attribute", "id", "0");
        //109306

//        int maxKeys = 109306;
//        int now = 0;
//        for (int i = 0; i < 2; ++i) {
//            for (int j = 0; j < maxKeys; ++j) {
//                String keywords = "";
//                int st = now / 10_0000;
//                ++now;
//                if (st == 0) {
//                    keywords = "a b";
//                } else if (st == 1) {
//                    keywords = "b c";
//                } else if (st == 2) {
//                    keywords = "c d";
//                }
////                System.out.println(Arrays.toString(ByteUtil.getKByte((long) st, 7)));
////                System.out.println(Arrays.toString(ByteUtil.longToByte(now)));
////                System.out.println(Arrays.toString(ByteUtil.concat(ByteUtil.getKByte(st, 7), ByteUtil.longToByte(now))));
//                hBaseUtil.put("testKeywords",
//                        ByteUtil.concat(ByteUtil.getKByte((long) st, 7), ByteUtil.longToByte(now)),
//                        "attr", "keywords", keywords);
//            }
//        }

        System.out.println(hBaseUtil.scanAll("testKeywords").size());
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"a"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"b"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"c"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"d"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());
        System.out.println(hBaseUtil.scanWithKeywords(
                "testKeywords",
                new String[]{"e"},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 1, 1, 1, 1, 1},
                null
        ).size());

//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "attr", "keywords", "a b c");
//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, "attr", "keywords", "c d");

//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}, "attr", "keywords", "e f");
//        hBaseUtil.put("testKeywords", new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}, "attr", "keywords", "g h");
//        System.out.println(hBaseUtil.scanWithKeywords(
//                "testKeywords",
//                new String[]{"a", "e"},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
//                null
//        ));
//        System.out.println(hBaseUtil.scanWithKeywords(
//                "testKeywords",
//                new String[]{"a"},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
//                null
//        ));
//        System.out.println(hBaseUtil.scanWithKeywords(
//                "testKeywords",
//                new String[]{"e"},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
//                null
//        ));
//        System.out.println(hBaseUtil.scanWithKeywords(
//                "testKeywords",
//                new String[]{"x"},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
//                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},
//                null
//        ));
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
