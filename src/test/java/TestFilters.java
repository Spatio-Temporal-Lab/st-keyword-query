import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.preProcessing.DataProcessor;
import org.urbcomp.startdb.stkq.preProcessing.QueryGenerator;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestFilters {
    private static int minT = Integer.MAX_VALUE;
    private static int maxT = -1;
    private static final List<Query> QUERIES = QueryGenerator.getQueries("queriesZipfSample.csv");

    private static final List<STObject> SAMPLE_DATA = DataProcessor.getSampleData();
    private static final ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
    private static final TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();
    private static final KeywordKeyGenerator keywordGenerator = new KeywordKeyGenerator();
    private static List<List<byte[]>> GROUND_TRUTH_RANGES = new ArrayList<>();

    @BeforeClass
    public static void initial() {
        IFilter setFilter = new SetFilter();
        insertIntoFilter(setFilter);
        GROUND_TRUTH_RANGES = shrinkByFilter(setFilter);
        for (Query query : QUERIES) {
            query.setQueryType(QueryType.CONTAIN_ONE);
        }
    }

    @Test
    public void testInfiniFilterFPR() {
        IFilter[] filters = new IFilter[]{
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
            filter.sacrifice();
            System.out.println(RamUsageEstimator.humanSizeOf(filter));
            for (int i = 0; i < n; ++i) {
                Assert.assertTrue(filter.search(i));
            }
        }
    }

    @Test
    public void testSTFilter() throws IOException {
        int sBits = 8;
        int tBits = 4;

        STKFilter stFilter = new STKFilter(sBits, tBits, new BasicFilterManager(3, 13));

        insertIntoSTFilter(stFilter);

        long start = System.currentTimeMillis();
        List<List<byte[]>> results = shrinkBySTFilter(stFilter);
        long end = System.currentTimeMillis();
        checkNoFalsePositive(results);
        System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter) + " " +
                RamUsageEstimator.humanSizeOf(stFilter));
        System.out.println("Filter Memory Usage: " + stFilter.ramUsage());
        System.out.println("query Time: " + (end - start));
        System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());
        System.out.println("------------------------------------------------------------------");
    }

    private static void insertIntoFilter(IFilter filter) {
        for (STObject object : SAMPLE_DATA) {
            byte[] spatialKey = spatialKeyGenerator.toBytes(object.getLocation());
            int t = timeKeyGenerator.toNumber(object.getTime());
            byte[] timeKey = timeKeyGenerator.numberToBytes(t);
            minT = Math.min(minT, t);
            maxT = Math.max(maxT, t);
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        keywordGenerator.toBytes(s),
                        spatialKey,
                        timeKey);
                filter.insert(key);
            }
        }
    }

    private static void insertIntoSTFilter(ISTKFilter stFilter) throws IOException {
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

    private List<List<byte[]>> shrinkBySTFilter(ISTKFilter stFilter) throws IOException {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : TestFilters.QUERIES) {
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
