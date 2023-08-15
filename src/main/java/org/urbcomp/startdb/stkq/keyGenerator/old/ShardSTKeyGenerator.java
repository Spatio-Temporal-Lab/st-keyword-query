package org.urbcomp.startdb.stkq.keyGenerator.old;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class ShardSTKeyGenerator extends AbstractSTKeyGenerator {
    private int shard = 0;
    private final static int MAX_SHARD = 3;

    public ShardSTKeyGenerator(SpatialKeyGenerator spatialKeyGenerator, TimeKeyGenerator timeKeyGenerator) {
        super(spatialKeyGenerator, timeKeyGenerator);
    }

    public int getByteCount() {
        return timeKeyGenerator.getByteCount() + spatialKeyGenerator.getByteCount() + 1;
    }

    @Override
    public byte[] toKey(STObject object) {
        byte[] shardKey = ByteUtil.getKByte(shard++ % MAX_SHARD, 1);
        byte[] objKey = ByteUtil.longToByte(object.getID());
        byte[] timeKey = timeKeyGenerator.toKey(object.getTime());
        byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
        return ByteUtil.concat(shardKey, timeKey, spatialKey, objKey);
    }

    @Override
    public List<Range<byte[]>> toKeyRanges(Query query) {
        List<Range<byte[]>> rangesTemp = new ArrayList<>();

        List<Range<byte[]>> timeRanges = timeKeyGenerator.toKeyRanges(query);
        List<Range<byte[]>> spatialRanges = spatialKeyGenerator.toKeyRanges(query);
        for (Range<byte[]> timeRange : timeRanges) {
            int timeRangeStart = ByteUtil.toInt(timeRange.getLow());
            int timeRangeEnd = ByteUtil.toInt(timeRange.getHigh());
            for (int i = timeRangeStart; i <= timeRangeEnd; ++i) {
                byte[] now = ByteUtil.getKByte(i, TIME_BYTE_COUNT);
                for (Range<byte[]> spatialRange : spatialRanges) {
                    rangesTemp.add(new Range<>(
                            ByteUtil.concat(now, spatialRange.getLow()),
                            ByteUtil.concat(now, spatialRange.getHigh())
                    ));
                }
            }
        }

        List<Range<byte[]>> ranges = new ArrayList<>();
        for (Range<byte[]> range : rangesTemp) {
            for (int i = 0; i < MAX_SHARD; ++i) {
                byte[] byteI = ByteUtil.getKByte(i, 1);
                ranges.add(
                        new Range<>(
                                ByteUtil.concat(byteI, range.getLow()),
                                ByteUtil.concat(byteI, range.getHigh())
                        )
                );
            }
        }
        return ranges;
    }
}
