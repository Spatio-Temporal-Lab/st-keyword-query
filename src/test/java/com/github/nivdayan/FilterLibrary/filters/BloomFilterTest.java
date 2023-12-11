package com.github.nivdayan.FilterLibrary.filters;

import org.junit.Assert;
import org.junit.Test;

public class BloomFilterTest {

    @Test
    public void buildFromBytes() {
        BloomFilter bf1 = new BloomFilter(100, 10);
        int n = 100;
        for (int i = 0; i < n; ++i) {
            bf1.insert(i, false);
        }
        for (int i = 0; i < n; ++i) {
            Assert.assertTrue(bf1.search(i));
        }
        int error1 = 0;
        for (int i = n; i < n + n; ++i) {
            if (bf1.search(i)) {
                ++error1;
            }
        }

        byte[] bytes = bf1.getArray();
        long numBits = bf1.getNum_bits();
        BloomFilter bf2 = new BloomFilter(bytes, 7, numBits);
        for (int i = 0; i < n; ++i) {
            Assert.assertTrue(bf2.search(i));
        }
        int error2 = 0;
        for (int i = n; i < n + n; ++i) {
            if (bf2.search(i)) {
                ++error2;
            }
        }

        Assert.assertEquals(error1, error2);
    }
}