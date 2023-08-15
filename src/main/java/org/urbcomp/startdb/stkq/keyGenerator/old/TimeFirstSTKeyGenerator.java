package org.urbcomp.startdb.stkq.keyGenerator.old;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class TimeFirstSTKeyGenerator extends AbstractSTKeyGenerator {

    public TimeFirstSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public int getByteCount() {
        return timeKeyGenerator.getByteCount() + spatialKeyGenerator.getByteCount();
    }

    @Override
    public byte[] toKey(STObject object) {
        byte[] timeKey = timeKeyGenerator.toKey(object.getTime());
        byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
        byte[] objKey = ByteUtil.longToByte(object.getID());
        return ByteUtil.concat(timeKey, spatialKey, objKey);
    }

    @Override
    public List<Range<byte[]>> toKeyRanges(Query query) {
        List<Range<byte[]>> ranges = new ArrayList<>();

        List<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        List<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);
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
