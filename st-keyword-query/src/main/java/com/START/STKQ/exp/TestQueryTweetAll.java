package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialFirstSTKeyGenerator;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.DateUtil;
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
import java.util.Objects;

public class TestQueryTweetAll {
    public static void main(String[] args) throws ParseException, InterruptedException, IOException, ClassNotFoundException {

        String tableName = "testTweet";
        String bloomPath = "/usr/data/bloom/tweetBloom.txt";
        String outPathName = "/usr/data/log/queryBloomLog.txt";

        ArrayList<Query> queries = QueryGenerator.getQueries();
//        ArrayList<Query> queries = new ArrayList<>();
//        //41.26362024 -75.89635038 2012-08-28 05:30:50 Happy sunflower top world largest rooftop farm fa Brooklyn Grange Brooklyn Navy Yard
//        queries.add(new Query(
//                41.26, 41.27, -75.90, -75.89,
//                DateUtil.getDate("2012-08-28 00:00:00"), DateUtil.getDate("2012-08-28 23:00:00"),
//                "boy"
//        ));

        FileInputStream fi = new FileInputStream(bloomPath);
        ObjectInputStream oi = new ObjectInputStream(fi);
        BloomFilter<byte[]> bloomFilter = (BloomFilter<byte[]>) oi.readObject();

        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
        keyGenerator1.setBloomFilter(bloomFilter);
        QueryProcessor[] processors = new QueryProcessor[]{
                new QueryProcessor(tableName, keyGenerator1, false),
//                new QueryProcessor(tableName, new SpatialFirstSTKeyGenerator(), false)
        };

        ArrayList<Integer> len1 = new ArrayList<>();
        ArrayList<Integer> len2 = new ArrayList<>();

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            System.out.println("query size: " + queries.size());
            for (int i = 0; i < 1; ++i) {
                long timeMethod = 0;

                int ii = 0;
                for (Query query : queries) {
//                    if (++ii > 5) {
//                        break;
//                    }
//                    System.out.println("query = " + query);
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processors[i].getResult(query, true);
                    len1.add(result.size());
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                writer.write("method " + i + " time: " + timeMethod + "\n");
                writer.write("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime() + "\n");
//                if (i == 1) {
//                    System.out.println("origin size: " + processors[i].getAllSize());
//                    System.out.println("filtered size: " + processors[i].getFilteredSize());
//                    System.out.println("origin count: " + processors[i].getAllCount());
//                    System.out.println("filtered count: " + processors[i].getFilteredCount());
//                    System.out.println("bloom time: " + processors[i].getQueryBloomTime());
//
//                    writer.write("origin size: " + processors[i].getAllSize() + "\n");
//                    writer.write("filtered size: " + processors[i].getFilteredSize() + "\n");
//                    writer.write("origin count: " + processors[i].getAllCount() + "\n");
//                    writer.write("filtered count: " + processors[i].getFilteredCount() + "\n");
//                    writer.write("bloom time: " + processors[i].getQueryBloomTime() + "\n");
//                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < queries.size(); ++i) {
            System.out.println(len1.get(i));
        }
//        for (int i = 0; i < queries.size(); ++i) {
//            System.out.println(len1.get(i) + " " + len2.get(i));
//            if (!Objects.equals(len1.get(i), len2.get(i))) {
//                System.err.println("error: " + len1.get(i) + " " + len2.get(i));
//            }
//        }

    }
}
