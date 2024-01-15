package org.urbcomp.startdb.stkq.io;

import junit.framework.TestCase;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.Arrays;
import java.util.List;

public class RedisIOTest extends TestCase {
    public void testRangeQuery() {
        int db = 0;
        int n = 10;
        byte[] name = new byte[]{0};
        for (int i = 0; i < n; ++i) {
            byte[] score = ByteUtil.getKByte(i, 4);
            RedisIO.zAdd(db, name, i, score);
        }
        for (int i = 0; i < n; ++i) {
            List<byte[]> result = RedisIO.rangeQuery(db, name, i, i + 1);
            for (byte[] bytes : result) {
                System.out.println(Arrays.toString(bytes));
            }
            System.out.println("---------------------");
        }
    }
}