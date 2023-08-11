package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Location;
import com.START.STKQ.util.ByteUtil;
import junit.framework.TestCase;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.geomesa.curve.NormalizedDimension;

import java.math.BigInteger;
import java.util.Random;

import static org.junit.Assert.assertNotEquals;

public class SpatialKeyGeneratorTest extends TestCase {

    public void testToKey() {
        SpatialKeyGenerator generator = new SpatialKeyGenerator();
        byte[] bytes1 = generator.toKey(new Location(0, 0));
        assertNotEquals(ByteUtil.getValue(bytes1), BigInteger.valueOf(0));
        byte[] bytes2 = generator.toKey(new Location(-90, -180));
        assertEquals(ByteUtil.getValue(bytes2), BigInteger.valueOf(0));
        byte[] bytes3 = generator.toKey(new Location(90, 180));
        assertEquals(ByteUtil.getValue(bytes3), BigInteger.valueOf((1 << 28) - 1));
    }

    public void testHilbertCurve() {
        int bitCount = 14;
        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(bitCount);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(bitCount);
        SmallHilbertCurve curve = HilbertCurve.small().bits(bitCount).dimensions(2);
        Random random = new Random();

        int queryCount = 10;
        double[] x = new double[queryCount];
        double[] y = new double[queryCount];
        for (int i = 0; i < 10; ++i) {
            x[i] = random.nextDouble() * 180 - 90;
            y[i] = random.nextDouble() * 360 - 180;
            System.out.println(x[i] + " " + y[i]);
            long index = curve.index(normalizedLat.normalize(x[i]), normalizedLon.normalize(y[i]));
            System.out.println(index);
            long[] coordinate = curve.point(index);
            System.out.println(normalizedLat.denormalize((int) coordinate[0]) + " " + normalizedLon.denormalize((int) coordinate[1]));
        }
    }

    public void testNormalize() {
        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(14);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(14);

        System.out.println(normalizedLat.normalize(-90));
        System.out.println(normalizedLat.normalize(-67.5));
        System.out.println(normalizedLat.normalize(-45));
        System.out.println(normalizedLat.normalize(-22.5));
        System.out.println(normalizedLat.normalize(0));
        System.out.println(normalizedLat.normalize(90));
        System.out.println(normalizedLat.normalize(67.5));
        System.out.println(normalizedLat.normalize(45));
        System.out.println(normalizedLat.normalize(22.5));

        int now = 0;
        for (int i = 0 ; i < 9; ++i) {
            for (int j = -1; j <= 1; ++j) {
                System.out.println(normalizedLat.denormalize(now + j));
            }
            now += 2048;
        }
    }
}