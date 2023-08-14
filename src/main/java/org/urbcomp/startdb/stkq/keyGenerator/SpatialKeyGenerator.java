package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.ZIndexRange;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.locationtech.geomesa.curve.Z2SFC;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SpatialKeyGenerator implements IKeyGenerator<Location> {
    private final Z2SFC z2;
    private static final int BYTE_COUNT = 4;

    private static final int PRECISION = 14;

    public SpatialKeyGenerator() {
        this(PRECISION);
    }

    public SpatialKeyGenerator(int precision) {
        z2 = new Z2SFC(precision);
    }

    public int getByteCount() {
        return BYTE_COUNT;
    }

    @Override
    public byte[] toKey(Location pt) {
        return ByteUtil.getKByte(getNumber(pt), BYTE_COUNT);
    }

    @Override
    public List<Range<byte[]>> toKeyRanges(Query query) {
        List<Range<byte[]>> ranges = new ArrayList<>();

        List<ZIndexRange> indexRangeList = scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getMinLon(), query.getMaxLon()),
                Tuple2.apply(query.getMinLat(), query.getMaxLat())
        ));

        for (ZIndexRange indexRange : indexRangeList) {
            ranges.add(new Range<>(
                    ByteUtil.getKByte(indexRange.low(), BYTE_COUNT),
                    ByteUtil.getKByte(indexRange.high(), BYTE_COUNT)
            ));
        }
        return ranges;
    }

    public List<Range<Long>> toRanges(Query query) {
        List<Range<Long>> ranges = new ArrayList<>();

        List<ZIndexRange> indexRangeList = scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getMinLon(), query.getMaxLon()),
                Tuple2.apply(query.getMinLat(), query.getMaxLat())
        ));

        for (ZIndexRange indexRange : indexRangeList) {
            ranges.add(new Range<>(indexRange.low(), indexRange.high()));
        }
        return ranges;
    }

    public long getNumber(Location pt) {
        return z2.index(pt.getLon(), pt.getLat(), true);
    }

}
