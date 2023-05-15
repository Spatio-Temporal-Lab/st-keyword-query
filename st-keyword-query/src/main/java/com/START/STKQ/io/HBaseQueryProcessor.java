package com.START.STKQ.io;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
    private static CountDownLatch cdl;

    static class ScanThread extends Thread {
        private final byte[] start;
        private final byte[] end;
        private final String tableName;
        private final QueryType queryType;
        private final ArrayList<String> keywords;
        private final List<Map<String, String>> result;

        public ScanThread(String tableName, byte[] s, byte[] e, QueryType queryType, ArrayList<String> keywords, List<Map<String, String>> list) {
            this.start = s;
            this.end = e;
            this.queryType = queryType;
            this.keywords = keywords;
            this.tableName = tableName;
            this.result = list;
        }

        //覆写线程的run方法
        @Override
        public void run() {
            result.addAll(hBaseUtil.scanWithKeywords(tableName, keywords, ByteUtil.tobyte(start), ByteUtil.tobyte(end), queryType));
            cdl.countDown();
        }
    }
    public static List<Map<String, String>> scan(String tableName, ArrayList<Range<byte[]>> ranges,
                                                         QueryType queryType, ArrayList<String> keywords) throws InterruptedException {

        List<Map<String, String>> sycResult = Collections.synchronizedList(new ArrayList<>());
        cdl = new CountDownLatch(ranges.size());
        ArrayList<ScanThread> threads = new ArrayList<>();
        for (Range<byte[]> range : ranges) {
            threads.add(new ScanThread(tableName, range.getLow(), range.getHigh(), queryType, keywords, sycResult));
        }
        for (ScanThread thread : threads) {
            thread.start();
        }
        cdl.await();
        return sycResult;
    }

}
