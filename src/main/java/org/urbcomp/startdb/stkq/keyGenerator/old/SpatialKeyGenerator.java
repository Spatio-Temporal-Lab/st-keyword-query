package org.urbcomp.startdb.stkq.keyGenerator.old;

import org.locationtech.geomesa.curve.Z2SFC;
import org.urbcomp.startdb.stkq.ZIndexRange;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.keyGenerator.old.IKeyGenerator;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

public class SpatialKeyGenerator implements IKeyGenerator<Location> {
    private final Z2SFC z2;
    private static final int BYTE_COUNT = Constant.SPATIAL_BYTE_COUNT;

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
        @SuppressWarnings("unchecked")
        List<ZIndexRange> indexRangeList = (List<ZIndexRange>) scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getMinLon(), query.getMaxLon()),
                Tuple2.apply(query.getMinLat(), query.getMaxLat())
        ));

        return indexRangeList.stream()
                .map(o -> new Range<>(ByteUtil.getKByte(o.low(), BYTE_COUNT), ByteUtil.getKByte(o.high(), BYTE_COUNT)))
                .collect(Collectors.toList());
    }

    public List<Range<Long>> toRanges(Query query) {
        @SuppressWarnings("unchecked")
        List<ZIndexRange> indexRangeList = (List<ZIndexRange>) scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getMinLon(), query.getMaxLon()),
                Tuple2.apply(query.getMinLat(), query.getMaxLat())
        ));

        return indexRangeList.stream().map(o -> new Range<>(o.low(), o.high())).collect(Collectors.toList());
    }

    public long getNumber(Location pt) {
        return z2.index(pt.getLon(), pt.getLat(), true);
    }

}
