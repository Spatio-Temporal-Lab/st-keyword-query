package com.START.STKQ.keyGenerator;

import com.START.STKQ.ZIndexRange;
import com.START.STKQ.model.Location;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import org.locationtech.geomesa.curve.Z2SFC;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SpatialKeyGenerator implements IKeyGenerator<Location> {
    private final static Z2SFC z2 = new Z2SFC(14);
    private final int BYTE_COUNT = 4;

    public int getByteCount() {
        return BYTE_COUNT;
    }

    public byte[] toKey(Location pt) {
        return ByteUtil.getKByte(getNumber(pt), BYTE_COUNT);
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        List<ZIndexRange> indexRangeList = scala.collection.JavaConverters.seqAsJavaList(z2.toRanges(
                Tuple2.apply(query.getLeft(), query.getRight()),
                Tuple2.apply(query.getUp(), query.getDown())
        ));

        for (ZIndexRange indexRange : indexRangeList) {
            ranges.add(new Range<>(
                    ByteUtil.getKByte(indexRange.low(), BYTE_COUNT),
                    ByteUtil.getKByte(indexRange.high(), BYTE_COUNT)
            ));
        }
        return ranges;
    }

    public Long getNumber(Location pt) {
        return z2.index(pt.getLon(), pt.getLat(), true);
    }

}