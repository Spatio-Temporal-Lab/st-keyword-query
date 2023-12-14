package org.urbcomp.startdb.stkq.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class ZipfGeneratorTest {

    @Test
    public void testOutput() {
        ZipfGenerator generator = new ZipfGenerator(100, 0.8);
        ArrayList<Integer> results = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            int x = generator.next();
            Assert.assertTrue(x <= 100 && x >= 0);
            results.add(x);
        }
        System.out.println(results);
        results.sort(Integer::compareTo);
        System.out.println(results);
    }

}