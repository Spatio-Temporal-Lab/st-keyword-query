package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Location;
import com.START.STKQ.util.ByteUtil;
import junit.framework.TestCase;

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
}