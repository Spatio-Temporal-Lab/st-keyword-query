package com.START.STKQ.model;


import com.google.common.hash.BloomFilter;

import java.util.Arrays;

public class LeafNode extends TreeNode {
    BloomFilter<byte[]> bloomFilter;

    public LeafNode(BloomFilter<byte[]> bloomFilter, MBR mbr) {
        super(mbr);
        this.bloomFilter = bloomFilter;
    }

    public BloomFilter<byte[]> getBloomFilter() {
        return bloomFilter;
    }

    public void add(byte[] code) {
//        System.out.println(getMbr());
//        System.out.println(Arrays.toString(code));
        bloomFilter.put(code);
    }
}