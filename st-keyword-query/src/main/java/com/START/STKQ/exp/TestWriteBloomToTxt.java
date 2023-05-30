package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.google.common.hash.BloomFilter;

import java.io.*;
import java.text.ParseException;

public class TestWriteBloomToTxt {
    public static void main(String[] args) throws ParseException, IOException {
//        main1();
        DataReader dataReader = new DataReader();

        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetAll.csv", 5000_0000, 0.001);

        String outputPath = "/usr/data/bloom/tweetBloom.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(bloomFilter);
    }

    public static void main1() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        BloomFilter<byte[]>[] bloomFilters = dataReader.generateBloomFilters("E:\\data\\tweet\\tweetAll.csv", 9, 103991699, 0.001);
        int n = bloomFilters.length;
        for (int i = 0; i < n; ++i) {
            String outputPath = "E:\\data\\blooms\\" + i + ".txt";
            FileOutputStream f = new FileOutputStream(outputPath);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(bloomFilters[i]);
        }

//        DataReader dataReader = new DataReader();
//        BloomFilter<byte[]>[] bloomFilters = dataReader.generateLevelBloomFilters("/usr/data/tweetAll.csv", 2, 5000000, 0.001);
//        int n = bloomFilters.length;
//        for (int i = 0; i < n; ++i) {
//            String outputPath = "E:\\data\\blooms\\level1\\" + i + ".txt";
//            FileOutputStream f = new FileOutputStream(outputPath);
//            ObjectOutputStream o = new ObjectOutputStream(f);
//            o.writeObject(bloomFilters[i]);
//        }
    }
}
