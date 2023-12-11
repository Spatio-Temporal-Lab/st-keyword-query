package org.urbcomp.startdb.stkq.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteUtilTest {

    @Test
    public void testToInt() {
        assertEquals(1, ByteUtil.toInt(new byte[]{1}));
        assertEquals(255, ByteUtil.toInt(new byte[]{-1}));
        assertEquals(257, ByteUtil.toInt(new byte[]{1, 1}));
        assertEquals(-1, ByteUtil.toInt(new byte[]{-1, -1, -1, -1}));
    }

    @Test
    public void testGetKByte() {
        long x = 4294967295L;
        byte[] bytes = ByteUtil.getKByte(x, 4);
        for (byte b : bytes) {
            assertEquals(b, -1);
        }
    }

    @Test
    public void testGetBytesCountByBitsCount() {
        for (int i = 1; i <= 8; ++i) {
            for (int j = 1; j <= 8; ++j) {
                assertEquals(i, ByteUtil.getBytesCountByBitsCount((i - 1) * 8 + j));
            }
        }
    }

    @Test
    public void testLongToBytes() {
        long i = 0;
        Assert.assertArrayEquals(ByteUtil.longToBytes(i), new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        i = Integer.MAX_VALUE;
        Assert.assertArrayEquals(ByteUtil.longToBytes(i), new byte[]{0, 0, 0, 0, 127, -1, -1, -1});
        i = Long.MAX_VALUE;
        Assert.assertArrayEquals(ByteUtil.longToBytes(i), new byte[]{127, -1, -1, -1, -1, -1, -1, -1});
        i = -1;
        Assert.assertArrayEquals(ByteUtil.longToBytes(i), new byte[]{-1, -1, -1, -1, -1, -1, -1, -1});
        for (long j = 1; j < 255; ++j) {
            Assert.assertArrayEquals(ByteUtil.longToBytes(j), new byte[]{0, 0, 0, 0, 0, 0, 0, (byte) j});
        }
    }

    @Test
    public void testLongToBytesWithoutPrefixZero() {
        long i = 0;
        Assert.assertArrayEquals(ByteUtil.longToBytesWithoutPrefixZero(i), new byte[]{0});
        i = Integer.MAX_VALUE;
        Assert.assertArrayEquals(ByteUtil.longToBytesWithoutPrefixZero(i), new byte[]{127, -1, -1, -1});
        i = Long.MAX_VALUE;
        Assert.assertArrayEquals(ByteUtil.longToBytesWithoutPrefixZero(i), new byte[]{127, -1, -1, -1, -1, -1, -1, -1});
        for (long j = 1; j < 255; ++j) {
            Assert.assertArrayEquals(ByteUtil.longToBytesWithoutPrefixZero(j), new byte[]{(byte) j});
        }
    }
}
