package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialFirstSTKeyGenerator;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.QueryGenerator;
import com.google.common.hash.BloomFilter;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;

public class TestQueryTweetAll {
    public static void main(String[] args) throws ParseException, InterruptedException, IOException, ClassNotFoundException {

        String tableName = "testTweetSample";
//        String bloomPath = "/usr/data/bloom/tweetBloom.txt";
//        String outPathName = "/usr/data/log/queryBloomLog.txt";
        String bloomPath = "/usr/data/bloom/multiBloom/";
        String outPathName = "/usr/data/log/querySampleBloomLog.txt";

        ArrayList<Query> queries = QueryGenerator.getQueries();
//        ArrayList<Query> queries = new ArrayList<>();
//        //41.26362024 -75.89635038 2012-08-28 05:30:50 Happy sunflower top world largest rooftop farm fa Brooklyn Grange Brooklyn Navy Yard
//        queries.add(new Query(
//                41.26, 41.27, -75.90, -75.89,
//                DateUtil.getDate("2012-08-28 00:00:00"), DateUtil.getDate("2012-08-28 23:00:00"),
//                "boy"
//        ));

        ArrayList<BloomFilter<byte[]>> bloomFilters = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            FileInputStream fi = new FileInputStream(bloomPath + i + ".txt");
            ObjectInputStream oi = new ObjectInputStream(fi);
            bloomFilters.add((BloomFilter<byte[]>) oi.readObject());
//            BloomFilter<byte[]> bloomFilter = (BloomFilter<byte[]>) oi.readObject();
        }


        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator(bloomFilters.get(1), bloomFilters.get(2));
//        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
        keyGenerator1.setBloomFilter(bloomFilters.get(0));
//        keyGenerator2.setBloomFilter(bloomFilters.get(0));
        QueryProcessor[] processors = new QueryProcessor[]{
                new QueryProcessor(tableName, keyGenerator1, false),
                new QueryProcessor(tableName, keyGenerator1, false),
                new QueryProcessor(tableName, keyGenerator1, true),
                new QueryProcessor(tableName, keyGenerator1, true),
        };

        ArrayList<ArrayList<Integer>> len = new ArrayList<>(processors.length);
        for (int i = 0; i < processors.length; ++i) {
            len.add(new ArrayList<>());
        }

        ArrayList<String> keywords111 = new ArrayList<>();
//        keywords111.add("12jkl");
        boolean f = false;
//        boolean f = true;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            System.out.println("query size: " + queries.size());
            for (int i = 0; i < processors.length; ++i) {
                long timeMethod = 0;

//                int ii = 0;
                for (Query query : queries) {
//                    if (++ii != 1999) {
//                        continue;
//                    }
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processors[i].getResult(query, f);
                    len.get(i).add(result.size());
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                f = !f;
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                System.out.println("method " + i + " query bloom time: " + processors[i].getQueryBloomTime());
                writer.write("method " + i + " time: " + timeMethod + "\n");
                writer.write("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime() + "\n");
                writer.write("method " + i + " query bloom time: " + processors[i].getQueryBloomTime() + "\n");
                System.out.println("origin size: " + processors[i].getAllSize());
                System.out.println("origin count: " + processors[i].getAllCount());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < processors.length; ++i) {
            for (int j = i + 1; j < processors.length; ++j) {
                if (!len.get(i).equals(len.get(j))) {
                    System.out.println(i + " " + j);
                }
            }
        }
        for (int i = 0; i < processors.length; ++i) {
            for (int j = 0; j < Math.min(len.get(i).size(), queries.size()); ++j) {
                if (j > 1000) {
                    System.out.print(len.get(i).get(j) + " ");
                }
            }
            System.out.println();
        }
//        for (int i = 0; i < queries.size(); ++i) {
//            System.out.println(len1.get(i));
//        }
//        for (int i = 0; i < queries.size(); ++i) {
//            System.out.println(len1.get(i) + " " + len2.get(i));
//            if (!Objects.equals(len1.get(i), len2.get(i))) {
//                System.err.println("error: " + len1.get(i) + " " + len2.get(i));
//            }
//        }

    }
}
