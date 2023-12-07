package org.urbcomp.startdb.stkq.initialization;

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

public class IntermediateFileGenerator {
    public static void main(String[] args) throws Exception {

//        writeInfiniFilter();
//        writeSTCount();
//        writeDistribution();
//        writeKeywords();
//        writeBf();
        writeKeywords("/usr/data/yelp.csv", "yelpKeywords.txt");
    }

    public static void writeBf() throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
        BloomFilter<byte[]> bloomFilter = dataProcessor.generateBloomFilter("/usr/data/tweetAll.csv", 50_000_000, 0.001);
        String outputPath = "/usr/data/bloom/multiBloom/all/tweetBloom.txt";
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(bloomFilter);
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

    public static void writeDistribution() throws IOException {
        DataProcessor dataProcessor = new DataProcessor();
        ArrayList<Map> maps = dataProcessor.generateDistribution("/usr/data/tweetAll.csv");
        System.out.println("st count: " + maps.get(0).size());
        System.out.println("st count: " + maps.get(1).size());

        try (FileOutputStream f = new FileOutputStream("/usr/data/st2Count.txt");
             ObjectOutputStream o = new ObjectOutputStream(f)) {
            o.writeObject(maps.get(0));
        }
        try (FileOutputStream f = new FileOutputStream("/usr/data/st2Words.txt");
             ObjectOutputStream o = new ObjectOutputStream(f)) {
            o.writeObject(maps.get(1));
        }
    }

    public static void writeKeywords(String in, String out) throws IOException {
        DataProcessor dataProcessor = new DataProcessor();
        Set<String> ss = dataProcessor.generateKeywords(in);

        String outputPath = "src/main/resources/" + out;
        FileOutputStream f = new FileOutputStream(outputPath);
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(ss);
    }
}
