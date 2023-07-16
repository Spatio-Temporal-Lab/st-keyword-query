package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Location;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.geomesa.curve.NormalizedDimension;

import java.io.Serializable;
import java.util.ArrayList;

public class HilbertSpatialKeyGenerator extends SpatialKeyGenerator implements Serializable {
    private final int BIT_COUNT = 14;
    private final int BYTE_COUNT = 4;
    private final int MAX_RANGE_COUNT = 32;
    NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(BIT_COUNT);
    NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(BIT_COUNT);
    private SmallHilbertCurve curve = HilbertCurve.small().bits(BIT_COUNT).dimensions(2);

    public HilbertSpatialKeyGenerator() {
        curve = HilbertCurve.small().bits(BIT_COUNT).dimensions(2);
    }

    public HilbertSpatialKeyGenerator(int precision) {
        curve = HilbertCurve.small().bits(precision).dimensions(2);
    }

    public int getByteCount() {
        return BYTE_COUNT;
    }

    public byte[] toKey(Location pt) {
        return ByteUtil.getKByte(getNumber(pt), BYTE_COUNT);
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        long[] point1 = new long[] {normalizedLat.normalize(query.getUp()), normalizedLon.normalize(query.getLeft())};
        long[] point2 = new long[] {normalizedLat.normalize(query.getDown()), normalizedLon.normalize(query.getRight())};
        Ranges rangesCurve = curve.query(point1, point2, MAX_RANGE_COUNT);

        for (org.davidmoten.hilbert.Range range : rangesCurve) {
            ranges.add(new Range<>(
                    ByteUtil.getKByte(range.low(), BYTE_COUNT),
                    ByteUtil.getKByte(range.high(), BYTE_COUNT)
            ));
        }
        return ranges;
    }

    public Long getNumber(Location pt) {
        return curve.index(normalizedLat.normalize(pt.getLat()), normalizedLon.normalize(pt.getLon()));
    }
}