package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class TSKeyGenerator implements ISTKeyGeneratorNew {

    private final ISpatialKeyGeneratorNew spatialKeyGenerator;
    private final TimeKeyGeneratorNew timeKeyGenerator;
    private final static int S_BYTE_COUNT = Constant.SPATIAL_BYTE_COUNT;
    private final static int T_BYTE_COUNT = Constant.TIME_BYTE_COUNT;
    private final static int S_BIT_COUNT = S_BYTE_COUNT * 8;

    public TSKeyGenerator() {
        this.spatialKeyGenerator = new HilbertSpatialKeyGeneratorNew();
        this.timeKeyGenerator = new TimeKeyGeneratorNew();
    }

    public TSKeyGenerator(ISpatialKeyGeneratorNew spatialKeyGenerator, TimeKeyGeneratorNew timeKeyGenerator) {
        this.spatialKeyGenerator = spatialKeyGenerator;
        this.timeKeyGenerator = timeKeyGenerator;
    }

    @Override
    public Long toNumber(STObject object) {
        int t = timeKeyGenerator.toNumber(object.getTime());
        long s = spatialKeyGenerator.toNumber(object.getLocation());
        return ((long) t << S_BIT_COUNT) | s;
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

        List<Range<Long>> tsRanges = new ArrayList<>();
        for (long t = tRangeLow; t <= tRangeHigh; ++t) {
            long tAfterShift = t << S_BIT_COUNT;
            for (Range<Long> sRange : sRanges) {
                tsRanges.add(new Range<>(tAfterShift | sRange.getLow(), tAfterShift | sRange.getHigh()));
            }
        }

        return tsRanges;
    }

    @Override
    public List<Range<byte[]>> toBytesRanges(Query query) {
        List<Range<Long>> sRanges = spatialKeyGenerator.toNumberRanges(query);
        List<Range<Integer>> tRanges = timeKeyGenerator.toNumberRanges(query);
        int tRangeLow = tRanges.get(0).getLow();
        int tRangeHigh = tRanges.get(0).getHigh();

        List<Range<byte[]>> tsRanges = new ArrayList<>();
        for (int t = tRangeLow; t <= tRangeHigh; ++t) {
            for (Range<Long> sRange : sRanges) {
                tsRanges.add(new Range<>(
                        ByteUtil.concat(timeKeyGenerator.numberToBytes(t), spatialKeyGenerator.numberToBytes(sRange.getLow())),
                        ByteUtil.concat(timeKeyGenerator.numberToBytes(t), spatialKeyGenerator.numberToBytes(sRange.getHigh()))));
            }
        }

        return tsRanges;
    }

    @Override
    public int getByteCount() {
        return S_BYTE_COUNT + T_BYTE_COUNT;
    }
}
