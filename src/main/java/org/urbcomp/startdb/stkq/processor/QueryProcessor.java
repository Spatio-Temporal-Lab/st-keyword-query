package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.AbstractSTFilter;
import org.urbcomp.startdb.stkq.filter.STFilter;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
//import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
//import org.urbcomp.startdb.stkq.keyGenerator.old.AbstractSTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.text.ParseException;
import java.util.*;

public class QueryProcessor {
    private final String tableName;
    private boolean filterInMemory = false;
    private STFilter stFilter;
    private STKeyGenerator stKeyGenerator;

    long queryHBaseTime = 0;
    long queryBloomTime = 0;

    long allSize = 0;
    long allCount = 0;

    public long getQueryBloomTime() {
        return queryBloomTime;
    }

    public long getAllSize() {
        return allSize;
    }

    public long getAllCount() {
        return allCount;
    }

    private long getRangeSize(Range<byte[]> range) {
        long left = ByteUtil.toLong(range.getLow());
        long right = ByteUtil.toLong(range.getHigh());
        return right - left + 1;
    }

    private long getRangesSize(List<Range<byte[]>> ranges) {
        long sum = 0;
        for (Range<byte[]> range : ranges) {
            sum += getRangeSize(range);
        }
        return sum;
    }

    public QueryProcessor(String tableName, STKeyGenerator keyGenerator) {
        this.tableName = tableName;
        this.stKeyGenerator = keyGenerator;
    }

    public QueryProcessor(String tableName, STFilter stFilter) {
        this.tableName = tableName;
        this.stFilter = stFilter;
        filterInMemory = true;
    }

    public long getQueryHBaseTime() {
        return queryHBaseTime;
    }

    public ArrayList<STObject> getResult(Query query) throws InterruptedException, ParseException {

        List<Map<String, String>> scanResults;

        List<Range<byte[]>> ranges;

        long begin = System.currentTimeMillis();
        if (filterInMemory) {
            ranges = stFilter.shrinkAndTransform(query);
        } else {
            ranges = stKeyGenerator.toBytesRanges(query);
        }
        long end = System.currentTimeMillis();
        queryBloomTime += end - begin;

        allSize += ranges.size();
        allCount += getRangesSize(ranges);

        begin = System.currentTimeMillis();
        scanResults = HBaseQueryProcessor.scan(tableName, ranges, query);
        end = System.currentTimeMillis();
        queryHBaseTime += end - begin;

        ArrayList<STObject> result = new ArrayList<>();
        for (Map<String, String> map : scanResults) {
            Location loc = new Location(map.get("loc"));
            Date date = DateUtil.getDate(map.get("time"));
            ArrayList<String> keywords = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));
            result.add(new STObject(Long.parseLong(map.get("id")), loc.getLat(), loc.getLon(), date, keywords));
        }

        return result;
    }

    public void close() {
        HBaseQueryProcessor.close();
    }
}
