package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.google.common.hash.BloomFilter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class TestFileIO {
    public static void main(String[] args) throws Exception {

//        writeInfiniFilter();
//        writeSTCount();
//        writeDistribution();
//        writeKeywords();
        writeBf();

//        DataReader dataReader = new DataReader();
//        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetSample.csv", 50000, 0.001);
//        String outputPath = "/usr/data/bloom/tweetSampleBloom.txt";
//        FileOutputStream f = new FileOutputStream(outputPath);
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        o.writeObject(bloomFilter);
    }

    public static void writeBf() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
//        ArrayList<BloomFilter<byte[]>> bloomFilters = dataReader.generateBloomFilters("/usr/data/tweetAll.csv", 50_000_000, 0.001);
        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetAll.csv", 50_000_000, 0.001);
//        int n = bloomFilters.size();
//        for (int i = 0; i < n; ++i) {
        String outputPath = "/usr/data/bloom/multiBloom/all/tweetBloom.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(bloomFilter);
//        }
    }

    public static void writeInfiniFilter() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        Map<BytesKey, ChainedInfiniFilter> filters = dataReader.generateSTDividedFilter("/usr/data/tweetAll.csv");
        System.out.println(filters.size());

        for (Map.Entry<BytesKey, ChainedInfiniFilter> entry : filters.entrySet()) {
            String outputPath = "/usr/data/bloom/dynamicBloom/all1/" + entry.getKey() + ".txt";
            entry.getValue().writeTo(Files.newOutputStream(Paths.get(outputPath)));
//            FileOutputStream f = new FileOutputStream(outputPath);
//            ObjectOutputStream o = new ObjectOutputStream(f);
//            o.writeObject(entry.getValue());
        }
    }

    public static void testSizeofInfiniFilter() {
        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 10);
        filter.set_expand_autonomously(true);

        for (int i = 0; i < 1000_0000; ++i) {
            filter.insert(i, false);
        }

        System.out.println(RamUsageEstimator.humanSizeOf(filter));
    }

    public static void writeSTCount() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        Map<BytesKey, Long> map = dataReader.generateCount("/usr/data/tweetAll.csv");
        System.out.println(map.size());

//        String outputPath = "/usr/data/count.txt";
//        FileOutputStream f = new FileOutputStream(outputPath);
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        o.writeObject(map);
    }

    public static void writeDistribution() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        ArrayList<Map> maps = dataReader.generateDistribution("/usr/data/tweetAll.csv");
        System.out.println("st count: " + maps.get(0).size());
        System.out.println("st count: " + maps.get(1).size());

        try(FileOutputStream f = new FileOutputStream("/usr/data/st2Count.txt");
            ObjectOutputStream o = new ObjectOutputStream(f)) {
            o.writeObject(maps.get(0));
        }
        try(FileOutputStream f = new FileOutputStream("/usr/data/st2Words.txt");
            ObjectOutputStream o = new ObjectOutputStream(f)) {
            o.writeObject(maps.get(1));
        }
    }

    public static void writeKeywords() throws ParseException, IOException {
        DataReader dataReader = new DataReader();
        Set<String> ss = dataReader.generateKeywords("/usr/data/tweetAll.csv");

        String outputPath = "/usr/data/keywords.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(ss);
    }
}
