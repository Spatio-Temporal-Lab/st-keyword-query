package org.urbcomp.startdb.stkq.exp;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.model.BytesKey;
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
        writeKeywords();
//        writeBf();

//        DataReader dataReader = new DataReader();
//        BloomFilter<byte[]> bloomFilter = dataReader.generateBloomFilter("/usr/data/tweetSample.csv", 50000, 0.001);
//        String outputPath = "/usr/data/bloom/tweetSampleBloom.txt";
//        FileOutputStream f = new FileOutputStream(outputPath);
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        o.writeObject(bloomFilter);
    }

    public static void writeBf() throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
//        ArrayList<BloomFilter<byte[]>> bloomFilters = dataReader.generateBloomFilters("/usr/data/tweetAll.csv", 50_000_000, 0.001);
        BloomFilter<byte[]> bloomFilter = dataProcessor.generateBloomFilter("/usr/data/tweetAll.csv", 50_000_000, 0.001);
//        int n = bloomFilters.size();
//        for (int i = 0; i < n; ++i) {
        String outputPath = "/usr/data/bloom/multiBloom/all/tweetBloom.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(bloomFilter);
//        }
    }

    public static void writeInfiniFilters() throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
        Map<BytesKey, ChainedInfiniFilter> filters = dataProcessor.generateSTDividedFilter("/usr/data/tweetAll.csv");
        System.out.println(filters.size());

        for (Map.Entry<BytesKey, ChainedInfiniFilter> entry : filters.entrySet()) {
            String outputPath = "/usr/data/bloom/dynamicBloom/all" + Constant.S_FILTER_ITEM_LEVEL + Constant.T_FILTER_ITEM_LEVEL + "/" + entry.getKey() + ".txt";
            entry.getValue().writeTo(Files.newOutputStream(Paths.get(outputPath)));
//            FileOutputStream f = new FileOutputStream(outputPath);
//            ObjectOutputStream o = new ObjectOutputStream(f);
//            o.writeObject(entry.getValue());
        }
    }

    public static void writeInfiniFilter() throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
//        ChainedInfiniFilter filter = dataProcessor.generateOneFilter("/usr/data/tweetAll.csv");
        ChainedInfiniFilter filter = dataProcessor.generateOneFilter(Constant.TWEET_DIR);
        System.out.println(RamUsageEstimator.humanSizeOf(filter));
//        String outputPath = "/usr/data/bloom/dynamicBloom/00.txt";
        String outputPath = Constant.DATA_DIR + "\\blooms\\00.txt";
        filter.writeTo(Files.newOutputStream(Paths.get(outputPath)));
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
        DataProcessor dataProcessor = new DataProcessor();
//        Map<BytesKey, Long> map = dataProcessor.generateCount("/usr/data/tweetAll.csv");
        Map<BytesKey, Long> map = dataProcessor.generateCount(Constant.TWEET_DIR);
        System.out.println(map.size());

//        String outputPath = "/usr/data/count.txt";
        String outputPath = Constant.DATA_DIR + "\\count.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(map);
    }

    public static void writeDistribution() throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
        ArrayList<Map> maps = dataProcessor.generateDistribution("/usr/data/tweetAll.csv");
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
        DataProcessor dataProcessor = new DataProcessor();
        Set<String> ss = dataProcessor.generateKeywords(Constant.TWEET_DIR);

        String outputPath = "src/main/resources/keywords.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(ss);
    }
}
