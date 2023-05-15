package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.io.DataReader;
import com.START.STKQ.keyGenerator.*;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.model.STQuadTree;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.QueryGenerator;
import com.github.StairSketch.StairBf;
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

public class TestBloomEfficiency {
    public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException {

//        int level = 5;
//        BloomFilter<byte[]>[] bloomFilters = new BloomFilter[level];
//        String bloomPath = "E:\\data\\blooms\\level\\";
//        for (int i = 0; i < level; ++i) {
//            FileInputStream fi = new FileInputStream(bloomPath + i + ".txt");
//            ObjectInputStream oi = new ObjectInputStream(fi);
//            bloomFilters[i] = (BloomFilter<byte[]>) oi.readObject();
//        }

        int level = 2;
        BloomFilter<byte[]>[] bloomFilters1 = new BloomFilter[level];
        String bloomPath = "E:\\data\\blooms\\level1\\";
        for (int i = 0; i < level; ++i) {
            FileInputStream fi = new FileInputStream(bloomPath + i + ".txt");
            ObjectInputStream oi = new ObjectInputStream(fi);
            bloomFilters1[i] = (BloomFilter<byte[]>) oi.readObject();
        }

        ArrayList<Query> queries = QueryGenerator.getQueries();

        String tableName = "testTweet";
        String bloomPath1 = "E:\\data\\tweetBloom.txt";
        String treePath = "E:\\data\\blooms\\treeFull.txt";
        String stairPath = "E:\\data\\blooms\\stair.txt";
        String outPathName = "E:\\data\\queryBloomLog.txt";

        FileInputStream fi = new FileInputStream(bloomPath1);
        ObjectInputStream oi = new ObjectInputStream(fi);
        BloomFilter<byte[]> bloomFilter = (BloomFilter<byte[]>) oi.readObject();

        fi = new FileInputStream(treePath);
        oi = new ObjectInputStream(fi);
        STQuadTree tree = (STQuadTree) oi.readObject();

        fi = new FileInputStream(stairPath);
        oi = new ObjectInputStream(fi);
        StairBf stairBf = (StairBf) oi.readObject();
        StairSTKeyGenerator stairSTKeyGenerator = new StairSTKeyGenerator(stairBf);
//        tree.setKeyGenerator(new SpatialFirstSTKeyGenerator());
//        DataReader dataReader = new DataReader();
//        STQuadTree tree = dataReader.generateTree("E:\\data\\tweet\\tweetAll.csv");

        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
        keyGenerator1.setBloomFilter(bloomFilter);
//        AbstractSTKeyGenerator keyGenerator2 = new LevelSTKeyGenerator(5, bloomFilters);
        AbstractSTKeyGenerator keyGenerator3 = new SLevelSTKeyGenerator(level, bloomFilters1);

//        tree.setKeyGenerator(new SpatialFirstSTKeyGenerator());
//        AbstractSTKeyGenerator keyGenerator2 = new TimeDividedSTKeyGenerator(new HilbertSpatialKeyGenerator(),
//                new TimeKeyGenerator(), bloomFilters);

        QueryProcessor[] processors = new QueryProcessor[]{
//                new QueryProcessor(tableName, tree),
                new QueryProcessor(tableName, keyGenerator1),
                new QueryProcessor(tableName, keyGenerator3),
//                new QueryProcessor(tableName, keyGenerator2),
//                new QueryProcessor(tableName, stairSTKeyGenerator)
//                new QueryProcessor(tableName, keyGenerator2),
        };

        ArrayList<Integer> len1 = new ArrayList<>();
        ArrayList<Integer> len2 = new ArrayList<>();
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            System.out.println("query size: " + queries.size());
            for (int i = 0; i < 2; ++i) {
                long timeMethod = 0;

                for (Query query : queries) {
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processors[i].getResult(query);
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                writer.write("method " + i + " time: " + timeMethod + "\n");
                writer.write("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime() + "\n");

                System.out.println("origin size: " + processors[i].getAllSize());
                System.out.println("filtered size: " + processors[i].getFilteredSize());
                System.out.println("origin count: " + processors[i].getAllCount());
                System.out.println("filtered count: " + processors[i].getFilteredCount());
                System.out.println("bloom time: " + processors[i].getQueryBloomTime());

                writer.write("origin size: " + processors[i].getAllSize() + "\n");
                writer.write("filtered size: " + processors[i].getFilteredSize() + "\n");
                writer.write("origin count: " + processors[i].getAllCount() + "\n");
                writer.write("filtered count: " + processors[i].getFilteredCount() + "\n");
                writer.write("bloom time: " + processors[i].getQueryBloomTime() + "\n");
            }
        } catch (IOException | ParseException | InterruptedException e) {
            throw new RuntimeException(e);
        }

//        for (int i = 0; i < 1000; ++i) {
//            if (!Objects.equals(len1.get(i), len2.get(i))) {
//                System.out.println("error: " + len1.get(i) + " " + len2.get(i));
//            }
//        }

//        long sumCount = 0;
//        long sumLength = 0;
//        for (Query query : queries) {
//            query.setQueryType(QueryType.CONTAIN_ONE);
//            ArrayList<Range<byte[]>> ranges = abstractSTKeyGenerator.toKeyRanges(query);
//            sumCount += ranges.size();
//            for (Range<byte[]> range : ranges) {
//                long left = ByteUtil.toLong(range.getLow());
//                long right = ByteUtil.toLong(range.getHigh());
//                sumLength += right - left + 1;
//            }
//        }
//        System.out.println(sumCount + " " + sumLength);
    }
}
