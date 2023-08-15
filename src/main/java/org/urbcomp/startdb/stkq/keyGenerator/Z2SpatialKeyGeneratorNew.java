package org.urbcomp.startdb.stkq.keyGenerator;

import org.locationtech.geomesa.curve.Z2SFC;
import org.urbcomp.startdb.stkq.ZIndexRange;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

public class Z2SpatialKeyGeneratorNew implements ISpatialKeyGeneratorNew {
    private final Z2SFC z2;
    private static final int BYTE_COUNT = Constant.SPATIAL_BYTE_COUNT;

    private static final int DEFAULT_PRECISION = 14;

    public Z2SpatialKeyGeneratorNew() {
        this(DEFAULT_PRECISION);
    }

    public Z2SpatialKeyGeneratorNew(int precision) {
        z2 = new Z2SFC(precision);
    }

    @Override
    public Long toNumber(Location object) {
        return z2.index(object.getLon(), object.getLat(), true);
    }

    @Override
    public byte[] toBytes(Location object) {
        return ByteUtil.getKByte(toNumber(object), BYTE_COUNT);
    }

    @Override
    public List<Range<Long>> toNumberRanges(Query query) {
        @SuppressWarnings("unchecked")
        List<ZIndexRange> indexRangeList = (List<ZIndexRange>) scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getMinLon(), query.getMaxLon()),
                Tuple2.apply(query.getMinLat(), query.getMaxLat())
        ));

        return indexRangeList.stream().map(o -> new Range<>(o.low(), o.high())).collect(Collectors.toList());
    }

    @Override
    public List<Range<byte[]>> toBytesRanges(Query query) {
        return toNumberRanges(query).stream()
                .map(o -> new Range<>(
                        ByteUtil.getKByte(o.getLow(), BYTE_COUNT),
                        ByteUtil.getKByte(o.getHigh(), BYTE_COUNT)))
                .collect(Collectors.toList());
    }
}
