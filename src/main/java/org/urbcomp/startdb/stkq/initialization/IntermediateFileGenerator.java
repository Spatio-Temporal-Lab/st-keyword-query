package org.urbcomp.startdb.stkq.initialization;

import org.urbcomp.startdb.stkq.io.DataProcessor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IntermediateFileGenerator {
    public static void main(String[] args) throws Exception {
        writeDistribution();
        writeKeywords("/usr/data/yelp.csv", "yelpKeywords.txt");
    }

    public static void writeDistribution() throws IOException {
        DataProcessor dataProcessor = new DataProcessor();
        List<Map> maps = dataProcessor.generateDistribution("/usr/data/tweetAll.csv");
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
