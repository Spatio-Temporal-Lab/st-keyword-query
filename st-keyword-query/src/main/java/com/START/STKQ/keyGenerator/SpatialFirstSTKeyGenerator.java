package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;

import java.util.ArrayList;

public class SpatialFirstSTKeyGenerator extends AbstractSTKeyGenerator {

    public SpatialFirstSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public SpatialFirstSTKeyGenerator() {
        super();
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
    public ArrayList<byte[]> toKeys(Query query) {
        ArrayList<byte[]> keys = new ArrayList<>();

        ArrayList<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);

        for (Range<byte[]> spatialRange : spatialRanges) {
            long spatialRangeStart = ByteUtil.toLong(spatialRange.getLow());
            long spatialRangeEnd = ByteUtil.toLong(spatialRange.getHigh());
            for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                byte[] now = ByteUtil.getKByte(i, SPATIAL_BYTE_COUNT);

                int timeRangeStart = ByteUtil.toInt(timeRanges.get(0).getLow());
                int timeRangeEnd = ByteUtil.toInt(timeRanges.get(0).getHigh());

                for (int j = timeRangeStart; j <= timeRangeEnd; ++j) {
                    keys.add(ByteUtil.concat(now, ByteUtil.getKByte(j, TIME_BYTE_COUNT)));
                }
            }
        }

        return keys;
    }
}