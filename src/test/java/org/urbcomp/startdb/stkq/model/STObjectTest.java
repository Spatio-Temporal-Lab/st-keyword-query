package org.urbcomp.startdb.stkq.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class STObjectTest {

    @Test
    public void testToByteArray() {
        Random rand = new Random(0);
        int testCount = 1000;
        for (int i = 0; i < testCount; ++i) {
            double lat = rand.nextDouble() * 180 - 90;
            double lon = rand.nextDouble() * 360 - 180;

            Date time = new Date(rand.nextInt(1_000_000));
            List<String> keywords = new ArrayList<>(Arrays.asList("ab", "b", "cde"));
            long id = rand.nextInt();
            STObject o1 = new STObject(id, lat, lon, time, keywords);

            byte[] array = o1.toByteArray();
            STObject o2 = new STObject(array);
            Assert.assertTrue(o1.equals(o2));
        }
    }
}