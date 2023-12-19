package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil;

    static ExecutorService service = Executors.newCachedThreadPool();

    static {
        hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
    }
    private static CountDownLatch cdl;

    static String[] keywords;

    static class ScanThread extends Thread {
        private final byte[] start;
        private final byte[] end;
        private final String tableName;
        private final Query query;
        private final List<Map<String, String>> result;

        public ScanThread(String tableName, byte[] s, byte[] e, Query query, List<Map<String, String>> result) {
            this.start = s;
            this.end = e;
            this.tableName = tableName;
            this.query = query;
            this.result = result;
        }

        //覆写线程的run方法
        @Override
        public void run() {
            List<Map<String, String>> scanResult = hBaseUtil.scanWithKeywords(tableName, keywords,
                    start, end, query.getQueryType());

            if (scanResult == null || scanResult.isEmpty()) {
                cdl.countDown();
                return;
            }

            for (Map<String, String> map : scanResult) {
                if (STKUtil.check(map, query)) {
                    result.add(map);
                }
            }

            cdl.countDown();
        }
    }

    static class BDIAScanThread extends Thread {
        private final byte[] start;
        private final byte[] end;
        private final String tableName;
        private final Query query;
        private final List<Map<String, String>> result;

        public BDIAScanThread(String tableName, byte[] s, byte[] e, Query query, List<Map<String, String>> result) {
            this.start = s;
            this.end = e;
            this.tableName = tableName;
            this.query = query;
            this.result = result;
        }

        //覆写线程的run方法
        @Override
        public void run() {
            List<Map<String, String>> scanResult = hBaseUtil.BDIAScan(tableName, keywords,
                    start, end, query.getQueryType());

            if (scanResult == null || scanResult.isEmpty()) {
                cdl.countDown();
                return;
            }

            for (Map<String, String> map : scanResult) {
                if (STKUtil.check(map, query)) {
                    result.add(map);
                }
            }

            cdl.countDown();
        }
    }

    public static List<Map<String, String>> scan(String tableName, List<Range<byte[]>> ranges,
                                                 Query query) throws InterruptedException {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }
        keywords = query.getKeywords().toArray(new String[0]);
        List<Map<String, String>> result = Collections.synchronizedList(new ArrayList<>());
        cdl = new CountDownLatch(ranges.size());
        for (Range<byte[]> range : ranges) {
            service.submit(new ScanThread(tableName,
                    ByteUtil.concat(range.getLow()),
                    ByteUtil.concat(range.getHigh(), new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}), query, result));
        }
        cdl.await();
        return result;
    }

    public static List<Map<String, String>> scanBDIA(String tableName, List<Range<byte[]>> ranges,
                                                 Query query) throws InterruptedException {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }
        keywords = query.getKeywords().toArray(new String[0]);
        List<Map<String, String>> result = Collections.synchronizedList(new ArrayList<>());
        cdl = new CountDownLatch(ranges.size());
        for (Range<byte[]> range : ranges) {
            service.submit(new BDIAScanThread(tableName,range.getLow(), range.getHigh(), query, result));
        }
        cdl.await();
        return result;
    }

    public static void close() {
        service.shutdown();
    }

}
