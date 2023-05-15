package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;

import java.util.ArrayList;

public class TimeFirstSTKeyGenerator extends AbstractSTKeyGenerator {

    public TimeFirstSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public TimeFirstSTKeyGenerator() {
    }

    public int getByteCount() {
        return timeKeyGenerator.getByteCount() + spatialKeyGenerator.getByteCount();
    }

    @Override
    public byte[] toKey(STObject object) {
        byte[] timeKey = timeKeyGenerator.toKey(object.getDate());
        byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
        byte[] objKey = ByteUtil.longToByte(object.getID());
        return ByteUtil.concat(timeKey, spatialKey, objKey);
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        ArrayList<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        ArrayList<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);
        for (Range<byte[]> timeRange : timeRanges) {
            int timeRangeStart = ByteUtil.toInt(timeRange.getLow());
            int timeRangeEnd = ByteUtil.toInt(timeRange.getHigh());
            for (int i = timeRangeStart; i <= timeRangeEnd; ++i) {
                byte[] now = ByteUtil.getKByte(i, TIME_BYTE_COUNT);
                for (Range<byte[]> spatialRange : spatialRanges) {
                    ranges.add(new Range<>(
                            ByteUtil.concat(now, spatialRange.getLow()),
                            ByteUtil.concat(now, spatialRange.getHigh())
                    ));
                }
            }
        }

        return ranges;
    }
}
