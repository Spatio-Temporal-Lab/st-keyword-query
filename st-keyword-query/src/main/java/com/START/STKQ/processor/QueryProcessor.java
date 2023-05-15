package com.START.STKQ.processor;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.io.HBaseQueryProcessor;
import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.model.Location;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.util.*;

//TODO: set spatial key generator type
public class QueryProcessor {
    private final String tableName;
    private final AbstractSTKeyGenerator generator;

    long queryHBaseTime = 0;
    long queryBloomTime = 0;

    long allSize = 0;
    long filteredSize = 0;
    long allCount = 0;
    long filteredCount = 0;

    public long getQueryBloomTime() {
        return queryBloomTime / 1000000;
    }

    public long getAllSize() {
        return allSize;
    }

    public long getFilteredSize() {
        return filteredSize;
    }

    public long getAllCount() {
        return allCount;
    }

    public long getFilteredCount() {
        return filteredCount;
    }

    private long getRangeSize(Range<byte[]> range) {
        long left = ByteUtil.toLong(range.getLow());
        long right = ByteUtil.toLong(range.getHigh());
        return right - left + 1;
    }

    private long getRangesSize(ArrayList<Range<byte[]>> ranges) {
        long sum = 0;
        for (Range<byte[]> range : ranges) {
            sum += getRangeSize(range);
        }
        return sum;
    }

    private long getRangesSize(ArrayList<Range<byte[]>> ranges, int len) {
        long sum = 0;
        for (Range<byte[]> range : ranges) {
            range.setLow(Arrays.copyOfRange(range.getLow(), 0, len));
            range.setHigh(Arrays.copyOfRange(range.getHigh(), 0, len));
            sum += getRangeSize(range);
        }
        return sum;
    }

    public QueryProcessor(String tableName, AbstractSTKeyGenerator rangeGenerator) {
        this.tableName = tableName;
        this.generator = rangeGenerator;
    }

    public long getQueryHBaseTime() {
        return queryHBaseTime;
    }

    public ArrayList<STObject> getResult(Query query) throws InterruptedException, ParseException {

        List<Map<String, String>> scanResults;

        ArrayList<Range<byte[]>> ranges = generator.toFilteredKeyRanges(query);

        QueryType queryType = query.getQueryType();
        ArrayList<String> queryKeywords = query.getKeywords();

        allSize += ranges.size();
        allCount += getRangesSize(ranges);

        long begin = System.currentTimeMillis();
//        scanResults = HBaseQueryProcessor.scan(tableName, ranges, queryType, queryKeywords);
        long end = System.currentTimeMillis();
        queryHBaseTime += end - begin;

//        ArrayList<STObject> result = new ArrayList<>();
//
//        for (Map<String, String> map : scanResults) {
//            Location loc = new Location(map.get("loc"));
//            if (!loc.in(query.getMBR())) {
//                continue;
//            }
//            Date date = DateUtil.getDate(map.get("time"));
//            if (date.before(query.getS()) || date.after(query.getT())) {
//                continue;
//            }
//            ArrayList<String> keywords = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));
//            if (queryType.equals(QueryType.CONTAIN_ONE)) {
//                boolean flag = false;
//                for (String s : query.getKeywords()) {
//                    if (keywords.contains(s)) {
//                        flag = true;
//                        break;
//                    }
//                }
//                if (!flag) {
//                    continue;
//                }
//            } else if (queryType.equals(QueryType.CONTAIN_ALL)) {
//                boolean flag = true;
//                for (String s : query.getKeywords()) {
//                    if (!keywords.contains(s)) {
//                        flag = false;
//                        break;
//                    }
//                }
//                if (!flag) {
//                    continue;
//                }
//            }
//
//            result.add(new STObject(Bytes.toLong(map.get("id").getBytes()), loc.getLat(), loc.getLon(), date, keywords));
//        }
//
        return new ArrayList<>();
    }
}
