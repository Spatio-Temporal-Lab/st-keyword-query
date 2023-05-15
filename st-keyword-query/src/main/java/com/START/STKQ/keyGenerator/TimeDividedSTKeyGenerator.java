package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import com.google.common.hash.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

//TODO: here we just use 4-12 month as the index, and should support all kinds of time windows length
public class TimeDividedSTKeyGenerator extends SpatialFirstSTKeyGenerator {

    private BloomFilter<byte[]>[] bloomFilters;

    public TimeDividedSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator,
                                     BloomFilter<byte[]>[] bloomFilters) {
        super(spatialKeyGenerator, timeKeyGenerator);
        this.bloomFilters = bloomFilters;
    }

    public TimeDividedSTKeyGenerator() {
    }

    public int getByteCount() {
        return timeKeyGenerator.getByteCount() + spatialKeyGenerator.getByteCount();
    }

    @Override
    public byte[] toKey(STObject object) {
        byte[] timeKey = timeKeyGenerator.toKey(object.getDate());
        byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
        byte[] objKey = ByteUtil.longToByte(object.getID());
        return ByteUtil.concat(spatialKey, timeKey, objKey);
    }

    public ArrayList<Range<byte[]>> filter(BloomFilter<byte[]> bloomFilter, Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        ArrayList<String> queryKeywords = query.getKeywords();
        QueryType queryType = query.getQueryType();
        ArrayList<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);

        for (Range<byte[]> spatialRange : spatialRanges) {
            long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow());
            long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh());

            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                byte[] sCode = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);

                for (Range<byte[]> timeRange : timeRanges) {
                    int timeLow = ByteUtil.toInt(timeRange.getLow());
                    int timeHigh = ByteUtil.toInt(timeRange.getHigh());

                    if (queryType.equals(QueryType.CONTAIN_ALL)) {
                        Queue<Integer> queue = new LinkedList<>();
                        for (int j = timeLow; j <= timeHigh; ++j) {
                            boolean containAll = true;
                            for (String qKeyword : queryKeywords) {
                                byte[] keywordCode = Bytes.toBytes(qKeyword.hashCode());
                                if (!bloomFilter.mightContain(ByteUtil.concat(keywordCode, sCode, ByteUtil.getKByte(j, 3)))) {
                                    containAll = false;
                                    break;
                                }
                            }
                            if (containAll) {
                                queue.add(j);
                            }
                        }
                        while (!queue.isEmpty()) {
                            int min = queue.poll();
                            int max = min;
                            while(!queue.isEmpty() && queue.peek() == max + 1) {
                                max = queue.poll();
                            }
                            ranges.add(new Range<>(
                                    ByteUtil.concat(sCode, ByteUtil.getKByte(min, 3)),
                                    ByteUtil.concat(sCode, ByteUtil.getKByte(max, 3))));
                        }
                    }
                    else if (queryType.equals(QueryType.CONTAIN_ONE)) {
                        Queue<Integer> queue = new LinkedList<>();
                        for (int j = timeLow; j <= timeHigh; ++j) {
                            boolean containOne = false;
                            for (String qKeyword : queryKeywords) {
                                byte[] keywordCode = Bytes.toBytes(qKeyword.hashCode());
                                if (bloomFilter.mightContain(ByteUtil.concat(keywordCode, sCode, ByteUtil.getKByte(j, 3)))) {
                                    containOne = true;
                                    break;
                                }
                            }
                            if (containOne) {
                                queue.add(j);
                            }
                        }
                        while (!queue.isEmpty()) {
                            int min = queue.poll();
                            int max = min;
                            while(!queue.isEmpty() && queue.peek() == max + 1) {
                                max = queue.poll();
                            }
                            ranges.add(new Range<>(
                                    ByteUtil.concat(sCode, ByteUtil.getKByte(min, 3)),
                                    ByteUtil.concat(sCode, ByteUtil.getKByte(max, 3))));
                        }
                    }
                }
            }
        }

        return ranges;
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        Date s = query.getS();
        Date t = query.getT();
        int monthS = s.getMonth();
        int monthT = t.getMonth();

        BloomFilter<byte[]> bloomFilter = bloomFilters[monthS - 3];
        if (monthS == monthT) {
            return filter(bloomFilter, query);
        } else {
            query.setS(s);
            query.setT(DateUtil.lastDayOfMonth(s));
            ranges.addAll(filter(bloomFilter, query));

            Date date = s;
            for (int i = monthS + 1; i < monthT; ++i) {
                bloomFilter = bloomFilters[i - 3];
                date.setMonth(i);
                query.setS(DateUtil.firstDayOfMonth(date));
                query.setT(DateUtil.lastDayOfMonth(date));
                ranges.addAll(filter(bloomFilter, query));
            }

            bloomFilter = bloomFilters[monthT - 3];
            query.setS(DateUtil.firstDayOfMonth(t));
            query.setT(t);
            ranges.addAll(filter(bloomFilter, query));
        }

        return ranges;
    }
}