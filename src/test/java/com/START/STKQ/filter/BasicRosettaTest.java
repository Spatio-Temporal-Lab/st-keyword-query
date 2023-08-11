package com.START.STKQ.filter;

import com.START.STKQ.model.Range;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Random;

public class BasicRosettaTest extends TestCase {
    Random random = new Random();

    @Test
    public void testFilter() {

        BasicRosetta basicRosetta = new BasicRosetta(5);
//        BasicRosetta basicRosetta = new BasicRosetta(2);
        int n = 100_0000;
        for (int i = -n; i <= n; ++i) {
            basicRosetta.insert(i);
        }
        System.out.println(basicRosetta);

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            int low = random.nextInt(n) - (n / 2);
            int high = low + random.nextInt(100);
            assertTrue(basicRosetta.filter(new Range<>((long) low, (long) high)).size() >= (high - low + 1));
        }

        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1 + " ms");

        long all = 0;
        long error = 0;
        t1 = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            ++all;
            int low = random.nextInt(n) + n + 1;
            int high = low + random.nextInt(100);
            if (basicRosetta.filter(new Range<>((long) low, (long) high)).size() > 0) {
                ++error;
            }
        }
        t2 = System.currentTimeMillis();
        System.out.println(t2 - t1 + " ms");
        System.out.println((double) error / all);
    }
}