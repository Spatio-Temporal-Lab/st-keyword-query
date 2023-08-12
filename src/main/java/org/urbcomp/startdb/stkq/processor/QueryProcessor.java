package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.AbstractSTKeyGenerator;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.text.ParseException;
import java.util.*;

//TODO: set spatial key generator type
public class QueryProcessor {
    private final String tableName;
    private final AbstractSTKeyGenerator generator;
    private final boolean filterInMemory;
    private final boolean filterInDb;
    private final boolean parallel;

    long queryHBaseTime = 0;
    long queryBloomTime = 0;

    long allSize = 0;
    long filteredSize = 0;
    long allCount = 0;
    long filteredCount = 0;

    public long getQueryBloomTime() {
        return queryBloomTime;
    }

    public long getFilterTime() {
        return generator.getFilterTime();
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

    public QueryProcessor(String tableName, AbstractSTKeyGenerator rangeGenerator, boolean filterInMemory,
                          boolean filterInDb, boolean parallel) {
        this.tableName = tableName;
        this.generator = rangeGenerator;
        this.filterInMemory = filterInMemory;
        this.filterInDb = filterInDb;
        this.parallel = parallel;
    }

    public long getQueryHBaseTime() {
        return queryHBaseTime;
    }

    public ArrayList<STObject> getResult(Query query) throws InterruptedException, ParseException {

        List<Map<String, String>> scanResults;

        ArrayList<Range<byte[]>> ranges;

        long begin = System.currentTimeMillis();
        if (filterInMemory) {
            ranges = generator.toFilteredKeyRanges(query);
        } else {
            ranges = generator.toKeyRanges(query);
        }
        long end = System.currentTimeMillis();
        queryBloomTime += end - begin;

        allSize += ranges.size();
        allCount += getRangesSize(ranges);

        begin = System.currentTimeMillis();
        scanResults = HBaseQueryProcessor.scan(tableName, ranges, query, filterInDb, parallel);
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
