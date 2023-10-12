import org.junit.Assert;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.STFilter;
import org.urbcomp.startdb.stkq.io.*;
import org.urbcomp.startdb.stkq.keyGenerator.*;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
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


        QueryProcessor[] processors = new QueryProcessor[]{
                new QueryProcessor(tableName, new STKeyGenerator())
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
        String tableName = "tweetSample";
        boolean tableExists = hBaseUtil.existsTable(tableName);
        ISTKeyGeneratorNew keyGenerator = new STKeyGenerator();
        List<STObject> objects = DataProcessor.getSampleData();

        STFilter filter = new STFilter(3, 12, 8, 4);
//        for (STObject object : objects) {
//            filter.insert(object);
//        }

        if (!tableExists) {
            hBaseUtil.createTable(tableName, new String[]{"attr"});// write data into HBase
            System.out.println("--------------------insert begin--------------------");
            HBaseIO.putObjects(tableName, keyGenerator, objects, 1000);
            System.out.println("--------------------insert end--------------------");
        }

        // test query results
        List<Query> queries = QueryGenerator.getQueries("queriesZipfSampleBig.csv");
        QueryProcessor[] queryProcessors = {
//                new QueryProcessor(tableName, keyGenerator),
                new QueryProcessor(tableName, filter)
        };
        System.out.println("--------------------query begin--------------------");

        int n = queryProcessors.length;

        long begin = System.currentTimeMillis();
        for (Query query : queries) {
            query.setQueryType(QueryType.CONTAIN_ONE);
//            List<STObject> correctResults = bruteForce(objects, query);
//            Collections.sort(correctResults);

            List<List<STObject>> resultsList = new ArrayList<>();
            for (QueryProcessor queryProcessor : queryProcessors) {
                List<STObject> results = queryProcessor.getResult(query);
                Collections.sort(results);
                resultsList.add(results);
            }
            for (int i = 1; i < n; ++i) {
                Assert.assertTrue(equals_(resultsList.get(0), resultsList.get(i)));
            }
//            System.out.println("**************************************");
//            System.out.println(query);
//            System.out.println(correctResults);
//            System.out.println(results);
//            Assert.assertTrue(equals_(correctResults, results));
        }
        long end = System.currentTimeMillis();
        System.out.println("--------------------query end--------------------");
        System.out.println((end - begin) + " ms");
        for (QueryProcessor processor : queryProcessors) {
            processor.close();
        }

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

    @Test
    public void test() throws ParseException {
        //38.91093026 -76.9358177 2012-04-02 12:00:53
        ISpatialKeyGeneratorNew sKeyGenerator = new HilbertSpatialKeyGeneratorNew();
        TimeKeyGeneratorNew tKeyGenerator = new TimeKeyGeneratorNew();
        System.out.println(Arrays.toString(sKeyGenerator.toBytes(new Location(38.91093026, -76.9358177))));
        System.out.println(Arrays.toString(tKeyGenerator.toBytes(DateUtil.getDate("2012-04-02 12:00:53"))));
        /*
        * [13, -9, -56, -73]
        * [1, -93, -108]
        * */
    }
}
