package org.urbcomp.startdb.stkq.io;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil;
//    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
//                System.out.println(map);
                if (STKUtil.check(map, query)) {
                    result.add(map);
                }
            }

            cdl.countDown();
        }
    }
    public static List<Map<String, String>> scan(String tableName, List<Range<byte[]>> ranges,
                                                 Query query) throws InterruptedException {

        keywords = query.getKeywords().toArray(new String[0]);
        List<Map<String, String>> result = Collections.synchronizedList(new ArrayList<>());
        cdl = new CountDownLatch(ranges.size());
        for (Range<byte[]> range : ranges) {
            service.submit(new ScanThread(tableName,
                    ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                    ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)), query, result));
        }
        cdl.await();
        return result;
    }

    public static void close() {
        service.shutdown();
    }

}
