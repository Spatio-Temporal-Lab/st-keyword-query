package com.START.STKQ.io;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Location;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        private final boolean useBfInHBase;
        private final String tableName;
        private final Query query;
        private final List<Map<String, String>> result;

        public ScanThread(String tableName, boolean useBfInHBase, byte[] s, byte[] e, Query query, List<Map<String, String>> list) {
            this.start = s;
            this.end = e;
            this.useBfInHBase = useBfInHBase;
            this.tableName = tableName;
            this.result = list;
            this.query = query;
        }

        //覆写线程的run方法
        @Override
        public void run() {
            String[] keywords = query.getKeywords().toArray(new String[0]);
            List<Map<String, String>> scanResult = hBaseUtil.scanWithKeywords(tableName, useBfInHBase, keywords,
                    ByteUtil.tobyte(start), ByteUtil.tobyte(end), query.getQueryType());
            result.addAll(scanResult);
            cdl.countDown();
        }
    }
    public static List<Map<String, String>> scan(String tableName, ArrayList<Range<byte[]>> ranges,
                                                 Query query, boolean useBfInHBase) throws InterruptedException {

        List<Map<String, String>> sycResult = Collections.synchronizedList(new ArrayList<>());
        cdl = new CountDownLatch(ranges.size());

        ExecutorService service = Executors.newFixedThreadPool(8);

        for (Range<byte[]> range : ranges) {
            service.submit(new ScanThread(tableName, useBfInHBase,
                            ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                            ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)), query, sycResult));
//            sycResult.addAll(hBaseUtil.scanWithKeywords(tableName, useBfInHBase, query.getKeywords().toArray(new String[0]),
//                    ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
//                    ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)),
//                    query.getQueryType()));
        }
        cdl.await();
        service.shutdown();
        return sycResult;
    }

}
