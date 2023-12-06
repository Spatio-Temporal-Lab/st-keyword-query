package org.urbcomp.startdb.stkq;

import org.junit.Assert;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.AbstractSTFilter;
import org.urbcomp.startdb.stkq.filter.STFilter;
import org.urbcomp.startdb.stkq.filter.StairBF;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.HFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.LRUFilterManager;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.AbstractQueryProcessor;
import org.urbcomp.startdb.stkq.processor.BasicQueryProcessor;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
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

    public static void testQueryCorrectness() throws ParseException, InterruptedException {
        // create table
        String tableName = "testTweet";
        ISTKeyGenerator keyGenerator = new STKeyGenerator();

        AbstractSTFilter[] filter = {
                new STFilter(8, 4, new BasicFilterManager(3, 12)),
                new STFilter(8, 4, new HFilterManager(3, 14)),
                new STFilter(8, 4, new LRUFilterManager(3, 14)),
        };

        StairBF bf = new StairBF(1, 1, 1, 1, 1);
        bf.init();

        // test query results
//        List<Query> queries = QueryGenerator.getQueries("queriesZipfBig.csv");
        List<Query> queries = QueryGenerator.getQueries("queriesZipfNew.csv");
        AbstractQueryProcessor[] queryProcessors = {
                new BasicQueryProcessor(tableName, keyGenerator),
                new QueryProcessor(tableName, filter[0]),
                new QueryProcessor(tableName, bf)
        };
        System.out.println("--------------------query begin--------------------");

        int n = queryProcessors.length;

        long begin = System.currentTimeMillis();
        for (Query query : queries) {
            query.setQueryType(QueryType.CONTAIN_ONE);

            List<List<STObject>> resultsList = new ArrayList<>();
            for (AbstractQueryProcessor queryProcessor : queryProcessors) {
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

        for (AbstractQueryProcessor processor : queryProcessors) {
            System.out.println(processor.getAllSize());
            processor.close();
        }
        for (AbstractSTFilter filter_ : filter) {
            System.out.println("filter ram size: " + filter_.ramUsage());
        }

        RedisIO.close();
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        testQueryCorrectness();
    }
}
