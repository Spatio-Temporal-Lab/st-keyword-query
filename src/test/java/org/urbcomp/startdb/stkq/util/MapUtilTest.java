package org.urbcomp.startdb.stkq.util;

import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class MapUtilTest extends TestCase {
    public void testCorrectness() {
        Map<Integer, Integer> x = new HashMap<>();
        int n = 10;
        for (int i = 0; i < n; ++i) {
            x.put(i, n - i);
        }
        System.out.println(MapUtil.sortByValue(x));

        x.clear();
        for (int i = 0; i < n; ++i) {
            x.put(i, i);
        }
        System.out.println(MapUtil.sortByValue(x));
    }
}
