package org.urbcomp.startdb.stkq.util;

import junit.framework.TestCase;

public class ByteUtilTest extends TestCase {
    public void testToInt() {
        assertEquals(1, ByteUtil.toInt(new byte[]{1}));
        assertEquals(255, ByteUtil.toInt(new byte[]{-1}));
        assertEquals(257, ByteUtil.toInt(new byte[]{1, 1}));
        assertEquals(-1, ByteUtil.toInt(new byte[]{-1, -1, -1, -1}));
    }

    public void testGetKByte() {
        long x = 4294967295L;
        byte[] bytes = ByteUtil.getKByte(x, 4);
        for (byte b : bytes) {
            assertEquals(b, -1);
        }
    }
}
