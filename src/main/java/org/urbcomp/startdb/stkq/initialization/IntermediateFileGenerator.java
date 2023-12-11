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
        writeKeywords("/usr/data/yelp.csv", "yelpKeywords.txt");
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
