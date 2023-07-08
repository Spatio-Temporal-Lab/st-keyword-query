package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialFirstSTKeyGenerator;
import com.START.STKQ.model.BytesKey;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.QueryGenerator;
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
        ArrayList<Query> queries = QueryGenerator.getQueries();

        ArrayList<BloomFilter<byte[]>> bloomFilters = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            FileInputStream fi = new FileInputStream(bloomPath + i + ".txt");
            ObjectInputStream oi = new ObjectInputStream(fi);
            bloomFilters.add((BloomFilter<byte[]>) oi.readObject());
        }

        AbstractSTKeyGenerator keyGenerator = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
        keyGenerator.setBloomFilter(bloomFilters.get(0));

        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
        keyGenerator1.setLoadFilterDynamically(true);

        boolean parallel = true;
        QueryProcessor[] processors = new QueryProcessor[]{
                new QueryProcessor(tableName, keyGenerator1, true, true, parallel),
//                new QueryProcessor(tableName, keyGenerator, true, true, parallel),
//                new QueryProcessor(tableName, keyGenerator, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator, false, true, parallel),
//                new QueryProcessor(tableName, keyGenerator, false, false, parallel),
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

        System.out.println(keyGenerator1.getFilters().size());
        for (Map.Entry<BytesKey, Filter> entry : keyGenerator1.getFilters().entrySet()) {
            System.out.println(RamUsageEstimator.humanSizeOf(entry.getValue()));
        }
        System.out.println(RamUsageEstimator.humanSizeOf(keyGenerator1.getFilters()));

//        for (ArrayList<ArrayList<STObject>> result_ : results) {
//            for (ArrayList<STObject> result : result_) {
//                Collections.sort(result);
//            }
//        }
//
//        for (int i = 0; i < processors.length; ++i) {
//            for (int j = i + 1; j < processors.length; ++j) {
//                if (!equals(results.get(i), results.get(j))) {
//                    System.out.println("result not equal: " + i + " " + j);
//                }
//                if (results.get(i).size() != results.get(j).size()) {
//                    System.out.println("count not equal: " + i + " " + j);
//                }
//            }
//        }
    }
}
