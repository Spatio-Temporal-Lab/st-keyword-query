package com.START.STKQ.io;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Location;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static {
        hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");
    }
    private static CountDownLatch cdl;

    static String[] keywords;

    static class ScanThread extends Thread {
        private final byte[] start;
        private final byte[] end;
        private final boolean useBfInHBase;
        private final String tableName;
        private final Query query;
        private final List<Map<String, String>> result;

        public ScanThread(String tableName, boolean useBfInHBase, byte[] s, byte[] e, Query query, List<Map<String, String>> result) {
            this.start = s;
            this.end = e;
            this.useBfInHBase = useBfInHBase;
            this.tableName = tableName;
            this.query = query;
            this.result = result;
        }

        //覆写线程的run方法
        @Override
        public void run() {
            List<Map<String, String>> scanResult = hBaseUtil.scanWithKeywords(tableName, useBfInHBase, keywords,
                    ByteUtil.tobyte(start), ByteUtil.tobyte(end), query.getQueryType());

            if (scanResult == null || scanResult.size() == 0) {
                cdl.countDown();
                return;
            }

            QueryType queryType = query.getQueryType();

            for (Map<String, String> map : scanResult) {

                Location loc = new Location(map.get("loc"));
                if (!loc.in(query.getMBR())) {
                    continue;
                }

                ArrayList<String> keywords_ = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));

                Date date;
                try {
                    synchronized (sdf) {
                        date = sdf.parse(map.get("time"));
                    }
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                if (date.before(query.getS()) || date.after(query.getT())) {
                    continue;
                }

                if (queryType.equals(QueryType.CONTAIN_ONE)) {
                    boolean flag = false;
                    for (String s : query.getKeywords()) {
                        if (keywords_.contains(s)) {
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        continue;
                    }
                } else if (queryType.equals(QueryType.CONTAIN_ALL)) {
                    boolean flag = true;
                    for (String s : query.getKeywords()) {
                        if (!keywords_.contains(s)) {
                            flag = false;
                            break;
                        }
                    }
                    if (!flag) {
                        continue;
                    }
                }

                result.add(map);
            }

            cdl.countDown();
        }
    }
    public static List<Map<String, String>> scan(String tableName, ArrayList<Range<byte[]>> ranges,
                                                 Query query, boolean useBfInHBase, boolean parallel) throws InterruptedException {

        keywords = query.getKeywords().toArray(new String[0]);
        List<Map<String, String>> result = Collections.synchronizedList(new ArrayList<>());

        if (parallel) {
            cdl = new CountDownLatch(ranges.size());
            ExecutorService service = Executors.newFixedThreadPool(8);
            for (Range<byte[]> range : ranges) {
                service.submit(new ScanThread(tableName, useBfInHBase,
                            ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                            ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)), query, result));
            }
            cdl.await();
            service.shutdown();
        }
        else {
            for (Range<byte[]> range : ranges) {
                result.addAll(hBaseUtil.scanWithKeywords(tableName, useBfInHBase, query.getKeywords().toArray(new String[0]),
                        ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                        ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)),
                        query.getQueryType()));
            }
        }
        return result;
    }

}
