package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class STKeyGenerator implements ISTKeyGenerator {

    private final ISpatialKeyGenerator spatialKeyGenerator;
    private final TimeKeyGenerator timeKeyGenerator;
    private final static int S_BYTE_COUNT = Constant.SPATIAL_BYTE_COUNT;
    private final static int T_BYTE_COUNT = Constant.TIME_BYTE_COUNT;
    private final static int T_BIT_COUNT = T_BYTE_COUNT * 8;

    public STKeyGenerator() {
        this.spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        this.timeKeyGenerator = new TimeKeyGenerator();
    }

    @Override
    public Long toNumber(STObject object) {
        long s = spatialKeyGenerator.toNumber(object.getLocation());
        int t = timeKeyGenerator.toNumber(object.getTime());
        return (s << T_BIT_COUNT) | t;
    }

    @Override
    public byte[] toBytes(STObject object) {
        return ByteUtil.getKByte(toNumber(object), S_BYTE_COUNT + T_BYTE_COUNT);
    }

    @Override
    public byte[] numberToBytes(Long number) {
        return ByteUtil.getKByte(number, S_BYTE_COUNT + T_BYTE_COUNT);
    }

    @Override
    public List<Range<Long>> toNumberRanges(Query query) {
        List<Range<Long>> sRanges = spatialKeyGenerator.toNumberRanges(query);
        List<Range<Integer>> tRanges = timeKeyGenerator.toNumberRanges(query);
        int tRangeLow = tRanges.get(0).getLow();
        int tRangeHigh = tRanges.get(0).getHigh();
        List<Range<Long>> stRanges = new ArrayList<>();
        for (Range<Long> sRange : sRanges) {
            for (long s = sRange.getLow(); s <= sRange.getHigh(); ++s) {
                long sAfterShift = s << T_BIT_COUNT;
                stRanges.add(new Range<>(sAfterShift | tRangeLow, sAfterShift | tRangeHigh));
            }
        }
        return stRanges;
    }

    @Override
    public List<Range<byte[]>> toBytesRanges(Query query) {
        List<Range<Long>> sRanges = spatialKeyGenerator.toNumberRanges(query);
        List<Range<Integer>> tRanges = timeKeyGenerator.toNumberRanges(query);
        int tRangeLow = tRanges.get(0).getLow();
        int tRangeHigh = tRanges.get(0).getHigh();
        List<Range<byte[]>> stRanges = new ArrayList<>();
        for (Range<Long> sRange : sRanges) {
            for (long s = sRange.getLow(); s <= sRange.getHigh(); ++s) {
                stRanges.add(new Range<>(
                        ByteUtil.concat(spatialKeyGenerator.numberToBytes(s), timeKeyGenerator.numberToBytes(tRangeLow)),
                        ByteUtil.concat(spatialKeyGenerator.numberToBytes(s), timeKeyGenerator.numberToBytes(tRangeHigh))));
            }
        }
        return stRanges;
    }

}
