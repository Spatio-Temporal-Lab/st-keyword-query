package org.urbcomp.startdb.stkq.keyGenerator;

import com.github.davidmoten.hilbert.hilbert.HilbertCurve;
import com.github.davidmoten.hilbert.hilbert.SmallHilbertCurve;
import org.locationtech.geomesa.curve.NormalizedDimension.NormalizedLat;
import org.locationtech.geomesa.curve.NormalizedDimension.NormalizedLon;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.List;
import java.util.stream.Collectors;

public class HilbertSpatialKeyGenerator implements ISpatialKeyGenerator {
    private final static int DEFAULT_PRECISION = 14;
    private final static int BYTE_COUNT = Constant.SPATIAL_BYTE_COUNT;
    private final static int MAX_RANGE_COUNT = 32;  // for each query range, return the maximum number of key ranges
    NormalizedLat normalizedLat = new NormalizedLat(DEFAULT_PRECISION);
    NormalizedLon normalizedLon = new NormalizedLon(DEFAULT_PRECISION);
    private final SmallHilbertCurve curve;

    public HilbertSpatialKeyGenerator() {
        this(DEFAULT_PRECISION);
    }

    public HilbertSpatialKeyGenerator(int precision) {
        curve = HilbertCurve.small().bits(precision).dimensions(2);
    }

    @Override
    public Long toNumber(Location object) {
        return curve.index(normalizedLat.normalize(object.getLat()), normalizedLon.normalize(object.getLon()));
    }

    @Override
    public byte[] toBytes(Location object) {
        return ByteUtil.getKByte(toNumber(object), BYTE_COUNT);
    }

    @Override
    public byte[] numberToBytes(Long number) {
        return ByteUtil.getKByte(number, BYTE_COUNT);
    }

    @Override
    public List<Range<Long>> toNumberRanges(Query query) {
        long[] p1 = new long[]{normalizedLat.normalize(query.getMinLat()), normalizedLon.normalize(query.getMinLon())};
        long[] p2 = new long[]{normalizedLat.normalize(query.getMaxLat()), normalizedLon.normalize(query.getMaxLon())};
        return curve.query(p1, p2, MAX_RANGE_COUNT).stream()
                .map(o -> new Range<>(o.low(), o.high()))
                .collect(Collectors.toList());
    }

    @Override
    public int getBits() {
        return DEFAULT_PRECISION + DEFAULT_PRECISION;
    }

    @Override
    public Location bytesToPoint(byte[] sKey) {
        long sKeyLong = ByteUtil.toLong(sKey);
        long[] originS = curve.point(sKeyLong);
        double lat = normalizedLat.denormalize((int) originS[0]);
        double lon = normalizedLon.denormalize((int) originS[1]);
        return new Location(lat, lon);
    }
}
