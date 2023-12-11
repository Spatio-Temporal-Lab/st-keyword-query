import org.apache.hadoop.hbase.client.ClientScanner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.AbstractSTFilter;
import org.urbcomp.startdb.stkq.filter.STFilter;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.HFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.LRUFilterManager;
import org.urbcomp.startdb.stkq.initialization.YelpFNSet;
import org.urbcomp.startdb.stkq.io.*;
import org.urbcomp.startdb.stkq.keyGenerator.*;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.AbstractQueryProcessor;
import org.urbcomp.startdb.stkq.processor.BDIAQueryProcessor;
import org.urbcomp.startdb.stkq.processor.BasicQueryProcessor;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestQuery {

    private static boolean equals_(List<STObject> a1, List<STObject> a2) {
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

    private static boolean equals(List<List<STObject>> a1, List<List<STObject>> a2) {
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

    @Test
    public void testQueryEfficiency() throws ParseException, InterruptedException {

        String tableName = "testTweet";
        String outPathName = Constant.DATA_DIR + "queryLog.txt";
        ArrayList<Query> queries = QueryGenerator.getQueries();


        long start;
        long end;
        start = System.currentTimeMillis();

        end = System.currentTimeMillis();
        System.out.println(end - start);


        BasicQueryProcessor[] processors = new BasicQueryProcessor[]{
                new BasicQueryProcessor(tableName, new STKeyGenerator())
        };

        List<List<List<STObject>>> results = new ArrayList<>(processors.length);
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
                    results.get(i).add(result);
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

        for (List<List<STObject>> result_ : results) {
            for (List<STObject> result : result_) {
                Collections.sort(result);
            }
        }

        for (int i = 0; i < processors.length; ++i) {

            int n = results.get(i).size();
            System.out.println("n = " + n);
            for (int j = 0; j < n; ++j) {
                System.out.println(results.get(i).get(j).size());
            }

            for (int j = i + 1; j < processors.length; ++j) {
                if (!equals(results.get(i), results.get(j))) {
                    System.out.println(results);
                    System.out.println("result not equal: " + i + " " + j);
                }
                if (results.get(i).size() != results.get(j).size()) {
                    System.out.println("count not equal: " + i + " " + j);
                }
            }
        }

        HBaseQueryProcessor.close();
    }

    @Test
    public void testQueryCorrectness() throws IOException, ParseException, InterruptedException {
        // create table
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
//        String tableName = "tweetSample";
//        String tableName1 = "testTweet";
//        String tableName2 = "testTweetBDIA";
        String tableName = "testYelp";

        ISTKeyGenerator keyGenerator1 = new STKeyGenerator();
        ISTKeyGenerator keyGenerator2 = new TSKeyGenerator();
        YelpFNSet.init();

        int sBits = 8;
        int tBits = 4;
        AbstractSTFilter[] filter = {
                new STFilter(sBits, tBits, new BasicFilterManager(3, 18)),
        };

        // test query results
        List<Query> queries = QueryGenerator.getQueries("yelpQueries.csv");
        AbstractQueryProcessor[] processors = {
//                new BasicQueryProcessor(tableName, keyGenerator1),
                new QueryProcessor(tableName, filter[0])
//                new BasicQueryProcessor(tableName1, keyGenerator1),
//                new BDIAQueryProcessor(tableName2, keyGenerator2)
//                new QueryProcessor(tableName1, filter[0]),
//                new QueryProcessor(tableName1, filter[2])
        };
        System.out.println("--------------------query begin--------------------");

        int n = processors.length;
        int ii = 0;

        long begin = System.currentTimeMillis();
        for (Query query : queries) {
//            if (!(++ii == 5592)) {
//                continue;
//            }
//            System.out.println(query);
            query.setQueryType(QueryType.CONTAIN_ONE);
//            if (++ii > 100) {
//                break;
//            }

            List<List<STObject>> resultsList = new ArrayList<>();
            for (AbstractQueryProcessor processor : processors) {
                List<STObject> results = processor.getResult(query);
                Collections.sort(results);
                resultsList.add(results);
            }
//            System.out.println(resultsList);

            for (int i = 1; i < n; ++i) {
                if (!equals_(resultsList.get(0), resultsList.get(i))) {
                    System.out.println(ii);
                    System.out.println(query);
                    System.out.println(resultsList.get(0));
                    System.out.println(resultsList.get(1));
                }
//                Assert.assertTrue(equals_(resultsList.get(0), resultsList.get(i)));
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("--------------------query end--------------------");
        System.out.println((end - begin) + " ms");

        for (AbstractQueryProcessor processor : processors) {
            System.out.println(processor.getAllSize());
            System.out.println(processor.getAllCount());
            processor.close();
        }
        for (AbstractSTFilter filter_ : filter) {
            System.out.println("filter ram size: " + filter_.ramUsage());
        }

        RedisIO.close();
    }

    @Test
    public void testSystem() throws IOException, ParseException, InterruptedException {
        // create table
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
        ISTKeyGenerator keyGenerator1 = new STKeyGenerator();
        ISTKeyGenerator keyGenerator2 = new TSKeyGenerator();
        List<STObject> objects = DataProcessor.getSampleData().subList(0, 1000);

        String tableName1 = "testTweetSample";
        if (!hBaseUtil.existsTable(tableName1)) {
            hBaseUtil.createTable(tableName1, new String[]{"attr"});// write data into HBase
            System.out.println("--------------------insert begin--------------------");
            HBaseIO.putObjects(tableName1, keyGenerator1, objects, 1000);
            System.out.println("--------------------insert end--------------------");
        }
        String tableName2 = "testTweetSampleBDIA";
        if (!hBaseUtil.existsTable(tableName2)) {
            hBaseUtil.createTable(tableName2, new String[]{"attr"});// write data into HBase
            System.out.println("--------------------insert begin--------------------");
            HBaseIO.putObjectsBDIA(tableName2, keyGenerator2, objects, 1000);
            System.out.println("--------------------insert end--------------------");
        }
//        else  {
//            hBaseUtil.truncateTable(tableName);
//        }

        // test query results
        List<Query> queries = QueryGenerator.getQueries("queriesZipfBig.csv");
        AbstractQueryProcessor processor1 = new BasicQueryProcessor(tableName1, keyGenerator1);
        AbstractQueryProcessor processor2 = new BDIAQueryProcessor(tableName2, keyGenerator2);

        System.out.println("--------------------query begin--------------------");

        long begin = System.currentTimeMillis();

        for (Query query : queries) {
            query.setQueryType(QueryType.CONTAIN_ONE);

            List<STObject> results = bruteForce(objects, query);
            Collections.sort(results);

            List<STObject> results1 = processor1.getResult(query);
            Collections.sort(results1);

            List<STObject> results2 = processor2.getResult(query);
            Collections.sort(results2);

            Assert.assertTrue(equals_(results, results1));
            Assert.assertTrue(equals_(results, results2));
        }
        long end = System.currentTimeMillis();
        System.out.println("--------------------query end--------------------");
        System.out.println((end - begin) + " ms");

        System.out.println(processor1.getAllSize());
        processor1.close();

        RedisIO.close();
    }

    List<STObject> bruteForce(List<STObject> objects, Query query) {
        List<STObject> results = new ArrayList<>();
        for (STObject object : objects) {
            if (STKUtil.check(object, query)) {
                results.add(object);
            }
        }
        return results;
    }
}
