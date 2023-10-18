import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestFilters {
//    private static final List<Query> QUERIES = QueryGenerator.getQueries("queriesZipfSample.csv");
    private static final List<Query> QUERIES = QueryGenerator.getQueries("queriesZipfSampleBig.csv");
    private static final List<Query> QUERIES_SMALL = QueryGenerator.getQueries("queriesZipfSample.csv");

    private static final List<STObject> SAMPLE_DATA = DataProcessor.getSampleData();
    private static final ISpatialKeyGeneratorNew spatialKeyGenerator = new HilbertSpatialKeyGeneratorNew();
    private static final TimeKeyGeneratorNew timeKeyGenerator = new TimeKeyGeneratorNew();
    private static final KeywordKeyGeneratorNew keywordGenerator = new KeywordKeyGeneratorNew();
    private static List<List<byte[]>> GROUND_TRUTH_RANGES = new ArrayList<>();

    @BeforeClass
    public static void initial() {
        IFilter setFilter = new SetFilter();
        insertIntoFilter(setFilter);
        GROUND_TRUTH_RANGES = shrinkByFilter(setFilter);
//        assertEquals(13365, GROUND_TRUTH_RANGES.stream().mapToInt(List::size).sum());
        for (Query query : QUERIES) {
            query.setQueryType(QueryType.CONTAIN_ONE);
        }
        for (Query query : QUERIES_SMALL) {
            query.setQueryType(QueryType.CONTAIN_ONE);
        }
    }

    @Test
    public void testInfiniFilterFPR() {

        IFilter[] filters = new IFilter[]{
//                new InfiniFilter(3, 12),
//                new InfiniFilter(3, 13),
//                new InfiniFilter(3, 14),
//                new InfiniFilter(3, 15),
//                new InfiniFilter(3, 16)
                new InfiniFilter(3, 20)
        };

        int id = 0;
        for (IFilter filter : filters) {
            long start = System.currentTimeMillis();
            insertIntoFilter(filter);
            long end = System.currentTimeMillis();
            System.out.println("Insert Time " + id++ +": " + (end - start));
        }

        id = 0;
        for (IFilter filter : filters) {
            long start = System.currentTimeMillis();
            List<List<byte[]>> results = shrinkByFilter(filter);
            long end = System.currentTimeMillis();
            System.out.println("Memory Usage: " + RamUsageEstimator.humanSizeOf(filter));
            System.out.println("Query Time " + id++ + ": " + (end - start));
            System.out.println("Result Size" + id + ": " + results.stream().mapToInt(List::size).sum());
            checkNoFalsePositive(results);
        }

    }

    @Test
    public void testSacrifice() {
        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 20);
        filter.set_expand_autonomously(true);

        int n = 10000;
        for (int i = 0; i < n; ++i) {
            filter.insert(i, false);
        }

        System.out.println(RamUsageEstimator.humanSizeOf(filter));

        for (int t = 0; t < 5; ++t) {
            System.out.println("start " + t);
            filter.sacrifice();
            System.out.println("end " + t);
            System.out.println(RamUsageEstimator.humanSizeOf(filter));
            for (int i = 0; i < n; ++i) {
                Assert.assertTrue(filter.search(i));
            }
        }
    }

    @Test
    public void testSTFilter() {
        int sBits = 8;
        int tBits = 4;
        AbstractSTFilter stFilter = new STFilter(3, 13, sBits, tBits);

        insertIntoSTFilter(stFilter);

        long start = System.currentTimeMillis();
        List<List<byte[]>> results = shrinkBySTFilter(stFilter, QUERIES_SMALL);
        long end = System.currentTimeMillis();
        checkNoFalsePositive(results);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter) + " " +
                RamUsageEstimator.humanSizeOf(stFilter));
        System.out.println("Filter Memory Usage: " + stFilter.size());
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());

        System.out.println("------------------------------------------------------------------");


        AbstractSTFilter stFilter1 = new HSTFilter(3, 14, sBits, tBits);
        insertIntoSTFilter(stFilter1);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter1) + " " + RamUsageEstimator.humanSizeOf(stFilter1));
        System.out.println("Filter Memory Usage: " + stFilter1.size());
        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter1, QUERIES_SMALL);
        end = System.currentTimeMillis();
        checkNoFalsePositive(results);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter1) + " " +
                RamUsageEstimator.humanSizeOf(stFilter1));
        System.out.println("Filter Memory Usage: " + stFilter1.size());
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());

        System.out.println("------------------------------------------------------------------");

        AbstractSTFilter stFilter2 = new AHSTFilter(3, 14, sBits, tBits);
        insertIntoSTFilter(stFilter2);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter2) + " " + RamUsageEstimator.humanSizeOf(stFilter2));
        System.out.println("Filter Memory Usage: " + stFilter2.size());
        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter2, QUERIES_SMALL);
        end = System.currentTimeMillis();
        checkNoFalsePositive(results);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter2) + " " +
                RamUsageEstimator.humanSizeOf(stFilter2));
        System.out.println("Filter Memory Usage: " + stFilter2.size());
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());

        System.out.println("******************************************************************");


        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter);
        end = System.currentTimeMillis();
        System.out.println("query Time: " + (end - start));
        System.out.println(results.stream().mapToInt(List::size).sum());

        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter1);
        end = System.currentTimeMillis();
        System.out.println("query Time: " + (end - start));
        System.out.println(results.stream().mapToInt(List::size).sum());

        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter2);
        end = System.currentTimeMillis();
        System.out.println("query Time: " + (end - start));
        System.out.println(results.stream().mapToInt(List::size).sum());
    }

    @Test
    public void testInfiniFilter() {
        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 12);
        filter.set_expand_autonomously(true);
        for (int i = 0; i <= 100; ++i) {
            boolean success = filter.insert(0, true);
            if (!success) {
                System.out.println(i);
            }
        }
    }

    @Test
    public void testSTFilterIO() throws IOException {
        int sBits = 8;
        int tBits = 4;
        AbstractSTFilter stFilter = new STFilter(3, 13, sBits, tBits);
//        insertIntoSTFilter(stFilter);
//        stFilter.out();
        List<List<byte[]>> results = shrinkBySTFilterWithIO(stFilter);
        checkNoFalsePositive(results);
    }

    private static void insertIntoFilter(IFilter filter) {
        for (STObject object : SAMPLE_DATA) {
            byte[] spatialKey = spatialKeyGenerator.toBytes(object.getLocation());
            byte[] timeKey = timeKeyGenerator.toBytes(object.getTime());
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        keywordGenerator.toBytes(s),
                        spatialKey,
                        timeKey);
                filter.insert(key);
            }
        }
    }

    private static void insertIntoSTFilter(AbstractSTFilter stFilter) {
        for (STObject object : SAMPLE_DATA) {
            stFilter.insert(object);
        }
    }

    private static List<List<byte[]>> shrinkByFilter(IFilter filter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES_SMALL) {
            List<byte[]> filterResult = filter.shrink(query, spatialKeyGenerator, timeKeyGenerator, keywordGenerator);
            results.add(filterResult);
        }
        return results;
    }

    private static List<byte[]> shrinkByFilter(IFilter filter, Query query) {
        return filter.shrink(query, spatialKeyGenerator, timeKeyGenerator, keywordGenerator);
    }

    private List<List<byte[]>> shrinkBySTFilter(AbstractSTFilter stFilter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<byte[]> filterResult = stFilter.shrink(query);
            results.add(filterResult);
        }
        return results;
    }

    private List<List<byte[]>> shrinkBySTFilterWithIO(AbstractSTFilter stFilter) throws IOException {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES_SMALL) {
            List<byte[]> filterResult = stFilter.shrinkWithIO(query);
            results.add(filterResult);
        }
        return results;
    }

    private List<byte[]> shrinkBySTFilter(AbstractSTFilter stFilter, Query query) {
        return stFilter.shrink(query);
    }

    private List<List<byte[]>> shrinkBySTFilter(AbstractSTFilter stFilter, List<Query> queries) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : queries) {
            List<byte[]> filterResult = stFilter.shrink(query);
            results.add(filterResult);
        }
        return results;
    }

    private static void checkNoFalsePositive(List<List<byte[]>> results) {
        for (int i = 0; i < QUERIES_SMALL.size(); ++i) {
            for (byte[] code : GROUND_TRUTH_RANGES.get(i)) {
                boolean find = false;
                for (byte[] aCode : results.get(i)) {
                    if (Arrays.equals(aCode, code)) {
                        find = true;
                        break;
                    }
                }
                assertTrue(find);
            }
        }
    }

}
