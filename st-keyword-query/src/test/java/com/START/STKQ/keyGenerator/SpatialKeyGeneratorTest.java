package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Location;
import com.START.STKQ.util.ByteUtil;
import junit.framework.TestCase;
import org.locationtech.geomesa.curve.NormalizedDimension;

import java.math.BigInteger;

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