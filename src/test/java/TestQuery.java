import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.old.AbstractSTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.SpatialFirstSTKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;

public class TestQuery {

    private static boolean equals_(ArrayList<STObject> a1, ArrayList<STObject> a2) {
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

    private static boolean equals(ArrayList<ArrayList<STObject>> a1, ArrayList<ArrayList<STObject>> a2) {
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

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {

//        String tableName = "testTweetSample";
//        String bloomPath = "/usr/data/bloom/multiBloom/sample/";
//        String outPathName = "/usr/data/log/querySampleBloomLog.txt";
//        ArrayList<Query> queries = QueryGenerator.getQueries("queriesForSample.csv");
//
        String tableName = "testTweet";
//        String bloomPath = "/usr/data/bloom/multiBloom/all/tweetBloom.txt";
//        String filterPath = "/usr/data/bloom/dynamicBloom/00.txt";
//        String outPathName = "/usr/data/log/queryBloomLog.txt";
        String filterPath = Constant.DATA_DIR + "\\blooms\\00.txt";
        String outPathName = Constant.DATA_DIR + "queryLog.txt";
        ArrayList<Query> queries = QueryGenerator.getQueries();
//        ArrayList<Query> queries = new ArrayList<>(QueryGenerator.getQueries().subList(1032, 1033));
//        ArrayList<Query> queries = QueryGenerator.getQueries("queriesZipf.csv");

//        FilterManager.init();
//        QueueFilterManager.init();

        long start;
        long end;
        start = System.currentTimeMillis();

//        BloomFilter<byte[]> bloomFilter;
//        FileInputStream fi = new FileInputStream(bloomPath);
//        ObjectInputStream oi = new ObjectInputStream(fi);
//        bloomFilter = (BloomFilter<byte[]>) oi.readObject();
//        oi.close();

        ChainedInfiniFilter chainedInfiniFilter = new ChainedInfiniFilter(0, 0);
        chainedInfiniFilter = chainedInfiniFilter.read(Files.newInputStream(Paths.get(filterPath)));

        end = System.currentTimeMillis();
        System.out.println(end - start);

//        AbstractSTKeyGenerator keyGenerator = new SpatialFirstSTKeyGenerator();
//        keyGenerator.setBloomFilter(bloomFilter);

        AbstractSTKeyGenerator keyGenerator1 = new SpatialFirstSTKeyGenerator();
//        keyGenerator1.setFilterType(FilterType.DYNAMIC);
        keyGenerator1.setFilter(chainedInfiniFilter);
//        keyGenerator1.setFlushStrategy(FlushStrategy.FIRST);

//        AbstractSTKeyGenerator keyGenerator2 = new SpatialFirstSTKeyGenerator();
//        keyGenerator2.setFilterType(FilterType.DYNAMIC);
//        keyGenerator2.setFlushStrategy(FlushStrategy.HOTNESS);

        boolean parallel = true;
        QueryProcessor[] processors = new QueryProcessor[]{
//                new QueryProcessor(tableName, keyGenerator, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator2, true, false, parallel),
                new QueryProcessor(tableName, keyGenerator1, true, false, parallel),
//                new QueryProcessor(tableName, keyGenerator, false, true, parallel),
//                new QueryProcessor(tableName, keyGenerator, true, false, parallel),
                new QueryProcessor(tableName, keyGenerator1, false, false, parallel),
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
                    results.get(i).add(result);
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                System.out.println("method " + i + " query bloom time: " + processors[i].getQueryBloomTime());
                System.out.println("method " + i + " filter time: " + processors[i].getFilterTime() / 100_0000);
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

//        switch (keyGenerator1.getFlushStrategy()) {
//            case HOTNESS:
//                System.out.println("reallocate count for 11: " + FilterManager.getReAllocateCount());
//                break;
//            case FIRST:
//                System.out.println("reallocate count for 12: " + QueueFilterManager.getReAllocateCount());
//                break;
//        }
//        switch (keyGenerator2.getFlushStrategy()) {
//            case HOTNESS:
//                System.out.println("reallocate count for 21: " + FilterManager.getReAllocateCount());
//                break;
//            case FIRST:
//                System.out.println("reallocate count for 22: " + QueueFilterManager.getReAllocateCount());
//                break;
//        }

//        System.out.println("load time hotness: " + FilterManager.getTime() / 100_0000);
//        System.out.println("load time queue: " + QueueFilterManager.getTime() / 100_0000);

        for (ArrayList<ArrayList<STObject>> result_ : results) {
            for (ArrayList<STObject> result : result_) {
                Collections.sort(result);
            }
        }

        for (int i = 0; i < processors.length; ++i) {
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

//        FilterManager.showSize();
    }
}
