package org.urbcomp.startdb.stkq;

import org.junit.Assert;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.BasicQueryProcessor;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.processor.StairQueryProcessor;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
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

    public static void testQueryCorrectness() throws IOException, ParseException, InterruptedException {
        // create table
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
        String tableName = "testTweet";
        ISTKeyGeneratorNew keyGenerator = new STKeyGenerator();

        AbstractSTFilter[] filter = {
                new STFilter(3, 12, 8, 4),
                new HSTFilter(3, 14, 8, 4),
                new LRUSTFilter(3, 14, 8, 4)
        };

        StairBF bf = new StairBF(1, 1, 1, 1, 1);
        bf.init();

        // test query results
//        List<Query> queries = QueryGenerator.getQueries("queriesZipfBig.csv");
        List<Query> queries = QueryGenerator.getQueries("queriesZipfNew.csv");
        BasicQueryProcessor[] queryProcessors = {
//                new QueryProcessor(tableName, keyGenerator),
                new QueryProcessor(tableName, filter[2]),
//                new StairQueryProcessor(tableName, bf)
        };
        System.out.println("--------------------query begin--------------------");

        int n = queryProcessors.length;

        long begin = System.currentTimeMillis();
        for (Query query : queries) {
            query.setQueryType(QueryType.CONTAIN_ONE);

            List<List<STObject>> resultsList = new ArrayList<>();
            for (BasicQueryProcessor queryProcessor : queryProcessors) {
                List<STObject> results = queryProcessor.getResult(query);
                Collections.sort(results);
                resultsList.add(results);
            }
            for (int i = 1; i < n; ++i) {
                Assert.assertTrue(equals_(resultsList.get(0), resultsList.get(i)));
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("--------------------query end--------------------");
        System.out.println((end - begin) + " ms");

        for (BasicQueryProcessor processor : queryProcessors) {
            System.out.println(processor.getAllSize());
            processor.close();
        }
        for (AbstractSTFilter filter_ : filter) {
            System.out.println("filter ram size: " + filter_.size());
        }

        RedisIO.close();
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        testQueryCorrectness();
    }
}
