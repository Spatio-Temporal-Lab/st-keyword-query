import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.filter.manager.AbstractFilterManager;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
                new InfiniFilter(3, 12),
                new InfiniFilter(3, 13),
                new InfiniFilter(3, 14),
                new InfiniFilter(3, 15),
                new InfiniFilter(3, 16)
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

//        InfiniFilter filter = (InfiniFilter) filters[filters.length - 1];
//        for (int i = 0; i < 5; ++i) {
//            filter.sacrifice();
//            System.out.println(shrinkByFilter(filter).stream().mapToInt(List::size).sum());
//        }

    }

    @Test
    public void testRangeFilters() {
        IRangeFilter[] filters = {
                new TRosetta(3),
                new SRosetta(3),
                new STRosetta(3)
        };

        for (IRangeFilter filter : filters) {
            long start = System.currentTimeMillis();
            insertIntoRangeFilter(filter);
            long end = System.currentTimeMillis();
            System.out.println(filter.getClass().getSimpleName() + " insert Time: " + (end - start));
        }

        for (IRangeFilter filter : filters) {
            long start = System.currentTimeMillis();
            List<List<byte[]>> results = shrinkByRangeFilter(filter);
            long end = System.currentTimeMillis();
            checkNoFalsePositive(results);

            String className = filter.getClass().getSimpleName();
            System.out.println(className + " query Time: " + (end - start));
            System.out.println(className + " result Size: " + results.stream().mapToInt(List::size).sum());
        }
    }

    @Test
    public void testSacrifice() {

        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 16);
        filter.set_expand_autonomously(true);

        int n = 100_0000;

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
        int sbits = 8;
        int tBits = 4;
        AbstractSTFilter stFilter = new STFilter(3, 13, sbits, tBits);

        insertIntoSTFilter(stFilter);

        long start = System.currentTimeMillis();/**/
        List<List<byte[]>> results = shrinkBySTFilter(stFilter);
        long end = System.currentTimeMillis();
        checkNoFalsePositive(results);
//
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter) + " " + RamUsageEstimator.humanSizeOf(stFilter));
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());


        AbstractSTFilter stFilter1 = new HSTFilter(3, 14, sbits, tBits);
        insertIntoSTFilter(stFilter1);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter1) + " " + RamUsageEstimator.humanSizeOf(stFilter1));
        start = System.currentTimeMillis();
        results = shrinkBySTFilter(stFilter1);
        end = System.currentTimeMillis();
        checkNoFalsePositive(results);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter1) + " " + RamUsageEstimator.humanSizeOf(stFilter1));
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());
//
//        results = shrinkBySTFilter(stFilter, QUERIES_SMALL);
//        System.out.println(results.stream().mapToInt(List::size).sum());
//        results = shrinkBySTFilter(stFilter1, QUERIES_SMALL);
//        System.out.println(results.stream().mapToInt(List::size).sum());
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
    public void testForError() {
        IFilter filter = new SetFilter();
        insertIntoFilter(filter);
        AbstractSTFilter filter1 = new STFilter(3, 13, 3, 2);
        insertIntoSTFilter(filter1);


        Query query = QUERIES.get(14180);
        List<byte[]> result = shrinkByFilter(filter, query);

        System.out.println("true: ");
        for (byte[] bytes : result) {
            System.out.print(Arrays.toString(bytes) + " ");
        }
        System.out.println();


        List<byte[]> result1 = shrinkBySTFilter(filter1, query);
        System.out.println("real: ");
        for (byte[] bytes : result1) {
            System.out.print(Arrays.toString(bytes) + " ");
        }
        System.out.println();

        for (byte[] bs : result) {
            boolean find = false;
            for (byte[] bs1 : result1) {
                if (Arrays.equals(bs, bs1)) {
                    find = true;
                    break;
                }
            }
            assertTrue(find);
        }

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

    private static void insertIntoRangeFilter(IRangeFilter filter) {
        for (STObject object : SAMPLE_DATA) {
            filter.insert(object);
        }
    }
    private static void insertIntoSTFilter(AbstractSTFilter stFilter) {
        for (STObject object : SAMPLE_DATA) {
            stFilter.insert(object);
        }
    }

    private static List<List<byte[]>> shrinkByFilter(IFilter filter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<byte[]> filterResult = filter.shrink(query, spatialKeyGenerator, timeKeyGenerator, keywordGenerator);
            results.add(filterResult);
        }
        return results;
    }

    private static List<byte[]> shrinkByFilter(IFilter filter, Query query) {
        return filter.shrink(query, spatialKeyGenerator, timeKeyGenerator, keywordGenerator);
    }

    private static List<List<byte[]>> shrinkByRangeFilter(IRangeFilter filter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<byte[]> filterResult = filter.shrink(query);
            results.add(filterResult);
        }
        return results;
    }

    private List<List<byte[]>> shrinkBySTFilter(AbstractSTFilter stFilter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<byte[]> filterResult = stFilter.shrink(query);
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
        for (int i = 0; i < QUERIES.size(); ++i) {
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
