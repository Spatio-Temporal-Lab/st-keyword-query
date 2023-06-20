package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.google.common.hash.BloomFilter;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;

public class TestWriteBloomToTxt {
    public static void main(String[] args) throws ParseException, IOException {

        main1();

//        DataReader dataReader = new DataReader();
//        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetSample.csv", 50000, 0.001);
//        String outputPath = "/usr/data/bloom/tweetSampleBloom.txt";
//        FileOutputStream f = new FileOutputStream(outputPath);
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        o.writeObject(bloomFilter);
    }

    public static void main1() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        ArrayList<BloomFilter<byte[]>> bloomFilters = dataReader.generateBloomFilter1("/usr/data/tweetSample.csv", 50000, 0.001);
        int n = bloomFilters.size();
        for (int i = 0; i < n; ++i) {
            String outputPath = "/usr/data/bloom/multiBloom/" + i + ".txt";
            FileOutputStream f = new FileOutputStream(outputPath);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(bloomFilters.get(i));
        }
    }
}
