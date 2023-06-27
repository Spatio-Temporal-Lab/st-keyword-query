package com.START.STKQ.keyGenerator;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.BfUtil;
import com.START.STKQ.util.ByteUtil;
import com.google.common.hash.BloomFilter;

import java.util.ArrayList;

public class SpatialFirstSTKeyGenerator extends AbstractSTKeyGenerator {
    BloomFilter<byte[]> sBf;
    BloomFilter<byte[]> tBf;

    public SpatialFirstSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public SpatialFirstSTKeyGenerator() {
        super();
    }

    public SpatialFirstSTKeyGenerator(BloomFilter<byte[]> sBf, BloomFilter<byte[]> tBf) {
        super();
        this.sBf = sBf;
        this.tBf = tBf;
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

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        ArrayList<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);
        for (Range<byte[]> spatialRange : spatialRanges) {
            long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow());
            long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh());
            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);
                for (Range<byte[]> timeRange : timeRanges) {
                    ranges.add(new Range<>(
                            ByteUtil.concat(now, timeRange.getLow()),
                            ByteUtil.concat(now, timeRange.getHigh())
                    ));
                }
            }
        }

        return ranges;
    }

    @Override
    public ArrayList<byte[]> toKeys(Query query, ArrayList<Range<byte[]>> sRanges, ArrayList<Range<byte[]>> tRanges) {
        ArrayList<byte[]> keys = new ArrayList<>();

        ArrayList<String> keywords = query.getKeywords();
        QueryType queryType = query.getQueryType();

        if (sBf != null && tBf != null) {
            ArrayList<byte[]> sCodes = new ArrayList<>();
            ArrayList<byte[]> tCodes = new ArrayList<>();

            for (Range<byte[]> spatialRange : sRanges) {
                long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow()) >>> 4;
                long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh()) >>> 4;

                for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                    if (BfUtil.check(sBf, ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT), keywords, queryType)) {
                        sCodes.add(ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT));
                    }
                }
            }

            int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> 2;
            int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> 2;

            for (int i = timeRangeStart; i <= timeRangeEnd; ++i) {
                if (BfUtil.check(tBf, ByteUtil.getKByte(i, TIME_BYTE_COUNT), keywords, queryType)) {
                    tCodes.add(ByteUtil.getKByte(i, TIME_BYTE_COUNT));
                }
            }

            for (byte[] sCode : sCodes) {
                for (byte[] tCode : tCodes) {
                    keys.add(ByteUtil.concat(sCode, tCode));
                }
            }

        } else {
            for (Range<byte[]> spatialRange : sRanges) {
                long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow()) >>> 4;
                long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh()) >>> 4;
                for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                    byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);
                    int timeRangeStart = ByteUtil.toInt(tRanges.get(0).getLow()) >>> 2;
                    int timeRangeEnd = ByteUtil.toInt(tRanges.get(0).getHigh()) >>> 2;
                    for (int j = timeRangeStart; j <= timeRangeEnd; ++j) {
                        keys.add(ByteUtil.concat(now, ByteUtil.getKByte(j, TIME_BYTE_COUNT)));
                    }
                }
            }
        }

        return keys;
    }
}