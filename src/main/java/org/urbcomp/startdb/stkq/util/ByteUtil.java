package org.urbcomp.startdb.stkq.util;

import java.math.BigInteger;

public class ByteUtil {
    public static byte[] getKByte(int num, int k) {
        k = Math.min(k, 4);
        byte[] targets = new byte[k];
        for (int i = k - 1; i >= 0; --i) {
            targets[k - 1 - i] = (byte) (num >>> (i << 3) & 0xFF);
        }
        return targets;
    }

    public static byte[] getKByte(long num, int k) {
        k = Math.min(k, 8);
        byte[] targets = new byte[k];
        for (int i = k - 1; i >= 0; --i) {
            targets[k - 1 - i] = (byte) (num >>> (i << 3) & 0xFF);
        }
        return targets;
    }

    public static byte[] longToByte(long num) {
        return getKByte(num, 8);
    }

    public static byte[] intToByte(int num) {
        return getKByte(num, 4);
    }

    public static byte[] concat(byte[]... values) {
        int lengthByte = 0;
        for (byte[] value : values) {
            lengthByte += value.length;
        }
        byte[] allBytes = new byte[lengthByte];
        int countLength = 0;
        for (byte[] b : values) {
            System.arraycopy(b, 0, allBytes, countLength, b.length);
            countLength += b.length;
        }
        return allBytes;
    }

    public static byte[] toByte(byte[] b) {
        byte[] result = new byte[b.length];
        System.arraycopy(b, 0, result, 0, b.length);
        return result;
    }

    public static int toInt(byte[] bytes) {
        int n = Math.min(bytes.length, 4);
        int result = 0;
        for (int i = 0; i < n; ++i) {
            result = result << 8 | (bytes[i] & 0xFF);
        }
        return result;
    }

    public static long toLong(byte[] bytes) {
        int n = Math.min(bytes.length, 8);
        long result = 0;
        for (int i = 0; i < n; ++i) {
            result = result << 8 | (bytes[i] & 0xFF);
        }
        return result;
    }

    public static BigInteger getValue(byte[] bytes) {
        return new BigInteger(1, toByte(bytes));
    }
}
