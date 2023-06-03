package com.START.STKQ.io;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil;
    static {
        hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");
    }
    private static CountDownLatch cdl;

    static class ScanThread extends Thread {
        private final byte[] start;
        private final byte[] end;
        private boolean useBfInHBase;
        private final String tableName;
        private final QueryType queryType;
        private final String[] keywords;
        private final List<Map<String, String>> result;

        public ScanThread(String tableName, boolean useBfInHBase, byte[] s, byte[] e, QueryType queryType, ArrayList<String> keywords, List<Map<String, String>> list) {
            this.start = s;
            this.end = e;
            this.queryType = queryType;
            this.useBfInHBase = useBfInHBase;
//            this.keywords = keywords;
            this.keywords = keywords.toArray(new String[0]);
            this.tableName = tableName;
            this.result = list;
        }

        //覆写线程的run方法
        @Override
        public void run() {
//            try {
//                result.addAll(hBaseUtil.scan(tableName, ByteUtil.tobyte(start), ByteUtil.tobyte(end)));
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            System.out.println(Arrays.toString(start) + " " + Arrays.toString(end));
            result.addAll(hBaseUtil.scanWithKeywords(tableName, useBfInHBase, keywords, ByteUtil.tobyte(start), ByteUtil.tobyte(end), queryType));
            cdl.countDown();
        }
    }
    public static List<Map<String, String>> scan(String tableName, ArrayList<Range<byte[]>> ranges,
                                                 QueryType queryType, ArrayList<String> keywords, boolean useBfInHBase) throws InterruptedException {

        List<Map<String, String>> sycResult = Collections.synchronizedList(new ArrayList<>());
//        cdl = new CountDownLatch(ranges.size());
//        ArrayList<ScanThread> threads = new ArrayList<>();
        for (Range<byte[]> range : ranges) {
            sycResult.addAll(hBaseUtil.scanWithKeywords(tableName, useBfInHBase, keywords.toArray(new String[0]),
                    ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                    ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)),
                    queryType));
//            threads.add(new ScanThread(tableName, useBfInHBase,
//                    ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
//                    ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)), queryType, keywords, sycResult));
        }
//        for (ScanThread thread : threads) {
//            thread.start();
//        }
//        cdl.await();
        return sycResult;
    }

}
