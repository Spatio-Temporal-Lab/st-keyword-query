package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.google.common.hash.BloomFilter;
import scala.util.control.Exception;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;

public class TestWriteBloomToTxt {
    public static void main(String[] args) throws ParseException, IOException, ClassNotFoundException {

//        main1();
        main2();

//        DataReader dataReader = new DataReader();
//        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetSample.csv", 50000, 0.001);
//        String outputPath = "/usr/data/bloom/tweetSampleBloom.txt";
//        FileOutputStream f = new FileOutputStream(outputPath);
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        o.writeObject(bloomFilter);
    }

    public static void main1() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        ArrayList<BloomFilter<byte[]>> bloomFilters = dataReader.generateBloomFilters("/usr/data/tweetAll.csv", 50_000_000, 0.001);
        int n = bloomFilters.size();
        for (int i = 0; i < n; ++i) {
            String outputPath = "/usr/data/bloom/multiBloom/all/" + i + ".txt";
            FileOutputStream f = new FileOutputStream(outputPath);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(bloomFilters.get(i));
        }
    }

    public static void main2() throws ParseException, IOException, ClassNotFoundException {
        DataReader dataReader = new DataReader();
        dataReader.setLimit(100);
        Map<BytesKey, ChainedInfiniFilter> filters = dataReader.generateSTDividedFilter("/usr/data/tweetAll.csv");
        System.out.println(filters.size());

        for (Map.Entry<BytesKey, ChainedInfiniFilter> entry : filters.entrySet()) {
            String outputPath = "/usr/data/bloom/dynamicBloom/all/" + entry.getKey() + ".txt";
            FileOutputStream f = new FileOutputStream(outputPath);
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(entry.getValue());
        }
    }
}
