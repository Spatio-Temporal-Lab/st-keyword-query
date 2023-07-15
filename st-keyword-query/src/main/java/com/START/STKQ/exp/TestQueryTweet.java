package com.START.STKQ.exp;

import com.START.STKQ.constant.FlushStrategy;
import com.START.STKQ.constant.QueryType;
import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialFirstSTKeyGenerator;
import com.START.STKQ.model.BytesKey;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.*;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.hash.BloomFilter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class TestQueryTweet {

    public static boolean equals_(ArrayList<STObject> a1, ArrayList<STObject> a2) {
        int n = a1.size();
        if (a2.size() != n) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            if (!a1.get(i).equals(a2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(ArrayList<ArrayList<STObject>> a1, ArrayList<ArrayList<STObject>> a2) {
        int n = a1.size();
        if (a2.size() != n) {
            return false;
        }
        boolean f = true;
        for (int i = 0; i < n; ++i) {
            if (!equals_(a1.get(i), a2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws ParseException, InterruptedException, IOException, ClassNotFoundException {

//        String tableName = "testTweetSample";
//        String bloomPath = "/usr/data/bloom/multiBloom/sample/";
//        String outPathName = "/usr/data/log/querySampleBloomLog.txt";
//        ArrayList<Query> queries = QueryGenerator.getQueries("queriesForSample.csv");
//
        String tableName = "testTweet";
        String bloomPath = "/usr/data/bloom/multiBloom/all/";
        String outPathName = "/usr/data/log/queryBloomLog.txt";
//        ArrayList<Query> queries = QueryGenerator.getQueries();
//        ArrayList<Query> queries = new ArrayList<>(QueryGenerator.getQueries().subList(1032, 1033));
        ArrayList<Query> queries = QueryGenerator.getQueries("queriesZipf.csv");

        FilterManager.init();
        QueueFilterManager.init();

        long start = 0;
        long end = 0;
        ArrayList<BloomFilter<byte[]>> bloomFilters = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            if (i == 0) start = System.currentTimeMillis();
            FileInputStream fi = new FileInputStream(bloomPath + i + ".txt");
            ObjectInputStream oi = new ObjectInputStream(fi);
            bloomFilters.add((BloomFilter<byte[]>) oi.readObject());
            if (i == 0) end = System.currentTimeMillis();
        }
        System.out.println(end - start);

        AbstractSTKeyGenerator keyGenerator = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
//        AbstractSTKeyGenerator keyGenerator = new SpatialFirstSTKeyGenerator();
        keyGenerator.setBloomFilter(bloomFilters.get(0));

        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
//        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
        keyGenerator1.setLoadFilterDynamically(true);
        keyGenerator1.setFlushStrategy(FlushStrategy.FIRST);

        AbstractSTKeyGenerator keyGenerator2 = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
//        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
        keyGenerator2.setLoadFilterDynamically(true);
        keyGenerator2.setFlushStrategy(FlushStrategy.HOTNESS);

        boolean parallel = true;
        QueryProcessor[] processors = new QueryProcessor[]{
//                new QueryProcessor(tableName, keyGenerator, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator2, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator1, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator, true, false, parallel),
                new QueryProcessor(tableName, keyGenerator, false, false, parallel),
//                new QueryProcessor(tableName, keyGenerator, false, true, parallel),
        };

        ArrayList<ArrayList<ArrayList<STObject>>> results = new ArrayList<>(processors.length);
        for (int i = 0; i < processors.length; ++i) {
            results.add(new ArrayList<>());
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            System.out.println("query size: " + queries.size());

            for (int i = 0; i < processors.length; ++i) {
                long timeMethod = 0;
                for (Query query : queries) {
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processors[i].getResult(query);
//                    results.get(i).add(result);
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                System.out.println("method " + i + " query bloom time: " + processors[i].getQueryBloomTime());
                System.out.println("origin size: " + processors[i].getAllSize());
                System.out.println("origin count: " + processors[i].getAllCount());

                writer.write("method " + i + " time: " + timeMethod + "\n");
                writer.write("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime() + "\n");
                writer.write("method " + i + " query bloom time: " + processors[i].getQueryBloomTime() + "\n");
                writer.write("origin size: " + processors[i].getAllSize() + "\n");
                writer.write("origin count: " + processors[i].getAllCount() + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        switch (keyGenerator1.getFlushStrategy()) {
            case HOTNESS:
                System.out.println("reallocate count for 11: " + FilterManager.getReAllocateCount());
                break;
            case FIRST:
                System.out.println("reallocate count for 12: " + QueueFilterManager.getReAllocateCount());
                break;
        }
        switch (keyGenerator2.getFlushStrategy()) {
            case HOTNESS:
                System.out.println("reallocate count for 21: " + FilterManager.getReAllocateCount());
                break;
            case FIRST:
                System.out.println("reallocate count for 22: " + QueueFilterManager.getReAllocateCount());
                break;
        }

//        for (ArrayList<ArrayList<STObject>> result_ : results) {
//            for (ArrayList<STObject> result : result_) {
//                Collections.sort(result);
//            }
//        }
//
//        for (int i = 0; i < processors.length; ++i) {
//            for (int j = i + 1; j < processors.length; ++j) {
//                if (!equals(results.get(i), results.get(j))) {
//                    System.out.println(results);
//                    System.out.println("result not equal: " + i + " " + j);
//                }
//                if (results.get(i).size() != results.get(j).size()) {
//                    System.out.println("count not equal: " + i + " " + j);
//                }
//            }
//        }

//        FilterManager.showSize();
    }
}
