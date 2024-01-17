package org.urbcomp.startdb.stkq.STILT;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

public class STILTTest extends TestCase {

    public void testQuery() {
        STILT trie = new STILT((byte) 64);
        int n = 2;
        long id = 0;
        for (int x = 0; x < n; ++x) {
            for (int y = 0; y < n; ++y) {
                for (int k = 0; k < n; ++k) {
                    for (int t = 0; t < n; ++t) {
                        trie.insert(crossEnc(x, y, k, t), id++);
                    }
                }
            }
        }

        id = 0;
        for (int x = 0; x < n; ++x) {
            for (int y = 0; y < n; ++y) {
                for (int k = 0; k < n; ++k) {
                    for (int t = 0; t < n; ++t) {
                        List<Long> result = trie.query(new QueryBox(x, x, y, y, t, t, k));
                        Assert.assertEquals(result.size(), 1);
                        Assert.assertEquals((long) result.get(0), id++);
                    }
                }
            }
        }
    }

    private long crossEnc(int a, int b, int c, int d) {
        long result = 0;
        for (int i = 15; i >= 0; --i) {
            result |= (long) (a >> i & 1) << (i << 2 | 3);
            result |= (long) (b >> i & 1) << (i << 2 | 2);
            result |= (long) (c >> i & 1) << (i << 2 | 1);
            result |= (long) (d >> i & 1) << (i << 2);
        }
        return result;
    }
}