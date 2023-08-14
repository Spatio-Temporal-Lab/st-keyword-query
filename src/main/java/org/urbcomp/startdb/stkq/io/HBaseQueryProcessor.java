package org.urbcomp.startdb.stkq.io;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseQueryProcessor {
    private final static HBaseUtil hBaseUtil;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static ExecutorService service = Executors.newCachedThreadPool();

    static {
        hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
//        hBaseUtil = new HBaseUtil();
//        hBaseUtil.init("192.168.137.204");
//        hBaseUtil.init("192.168.137.207");
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
                    start, end, query.getQueryType());

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
                if (date.before(query.getStartTime()) || date.after(query.getEndTime())) {
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
    public static List<Map<String, String>> scan(String tableName, List<Range<byte[]>> ranges,
                                                 Query query, boolean useBfInHBase, boolean parallel) throws InterruptedException {

        keywords = query.getKeywords().toArray(new String[0]);
        List<Map<String, String>> result = Collections.synchronizedList(new ArrayList<>());

        if (parallel) {
            cdl = new CountDownLatch(ranges.size());
            for (Range<byte[]> range : ranges) {
                service.submit(new ScanThread(tableName, useBfInHBase,
                            ByteUtil.concat(range.getLow(), ByteUtil.longToByte(0)),
                            ByteUtil.concat(range.getHigh(), ByteUtil.longToByte(Long.MAX_VALUE)), query, result));
            }
            cdl.await();
//            service.shutdown();
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

    public static void close() {
        service.shutdown();
    }

}
