import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.filter.manager.AHFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.filter.manager.HFilterManager;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
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
    private static int minT = Integer.MAX_VALUE;
    private static int maxT = -1;
    private static final List<Query> QUERIES = QueryGenerator.getQueries("queriesZipfSampleBig.csv");
    private static final List<Query> QUERIES_SMALL = QueryGenerator.getQueries("queriesZipfSample.csv");

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
    public void testStairBf() {
        int minT = 0;
        int maxT = 2_0000;
        int level = 8;
        StairBF stairBF = new StairBF(level, 20, 10, minT, maxT);
        for (int i = minT; i <= maxT; ++i) {
            stairBF.insert(intToByte4(i), i);
        }
        for (int i = minT; i <= maxT; ++i) {
            assertTrue(stairBF.query(intToByte4(i), i, i));
            assertTrue(stairBF.query(intToByte4(i), i));
        }
        int error = 0;
        for (int i = minT; i <= maxT - 2; ++i) {
            if (stairBF.query(intToByte4(i), i + 1, i + 2)) {
                ++error;
            }
        }
        System.out.println((double) error / (maxT - 5 - minT + 1));
        List<Integer> ids = new ArrayList<>();
        ids.add(minT);
        for (int i = 0; i < level - 1; ++i) {
            ids.add((ids.get(ids.size() - 1) + maxT) / 2);
        }
        for (int i = minT; i <= maxT; ++i) {
            int errorCount = 0;
            int errorCount1 = 0;
            for (int j = minT; j <= maxT; ++j) {
                if (i == j) continue;
                if (stairBF.query(intToByte4(j), i, i)) {
                    ++errorCount;
                }
                if (stairBF.query(intToByte4(j), i)) {
                    ++errorCount1;
                }
            }
            Assert.assertEquals(errorCount, errorCount1);
            if (ids.contains(i)) {
                System.out.println(i + " error rate = " + errorCount + " " + (double) errorCount / (maxT - minT + 1));
            }
        }
        System.out.println(RamUsageEstimator.humanSizeOf(stairBF));
    }

    @Test
    public void testStairBfShrink() throws IOException {
        System.out.printf("#%d %d\n", minT, maxT);
        ISTKFilter sbf = new StairBF(8, 10000, 20, minT, maxT);

        long start = System.currentTimeMillis();
        insertIntoSTFilter(sbf);
        long end = System.currentTimeMillis();
        System.out.println("Insert Time " +": " + (end - start));

        start = System.currentTimeMillis();
        List<List<byte[]>> results = shrinkBySTFilter(sbf);
        end = System.currentTimeMillis();

        System.out.println("Memory Usage: " + RamUsageEstimator.humanSizeOf(sbf));
        System.out.println("Query Time " + ": " + (end - start));
        System.out.println("Result Size" + ": " + results.stream().mapToInt(List::size).sum());
        checkNoFalsePositive(results);
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

        HFilterManager hFilterManager = new HFilterManager(3, 14);
//        AHFilterManager ahFilterManager = new AHFilterManager(3, 14);

        AbstractSTFilter[] stFilters = {
                new STFilter(sBits, tBits, new BasicFilterManager(3, 13)),
                new STFilter(sBits, tBits, hFilterManager),
//                new STFilter(sBits, tBits, ahFilterManager),
        };

        for (AbstractSTFilter stFilter : stFilters) {
            insertIntoSTFilter(stFilter);
        }

//        hFilterManager.build();
//        ahFilterManager.build();

        for (AbstractSTFilter stFilter : stFilters) {
            long start = System.currentTimeMillis();
            List<List<byte[]>> results = shrinkBySTFilter(stFilter, QUERIES_SMALL);
            long end = System.currentTimeMillis();
            checkNoFalsePositive(results);
            System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter) + " " +
                    RamUsageEstimator.humanSizeOf(stFilter));
            System.out.println("Filter Memory Usage: " + stFilter.ramUsage());
            System.out.println("query Time: " + (end - start));
            System.out.println("result Size: " + results.stream().mapToInt(List::size).sum());
            System.out.println("------------------------------------------------------------------");
        }

        for (int i = 1; i < stFilters.length; ++i) {
            stFilters[i].train(QUERIES_SMALL);
        }

        for (AbstractSTFilter stFilter : stFilters) {
            long start = System.currentTimeMillis();
            List<List<byte[]>> results = shrinkBySTFilter(stFilter);
            long end = System.currentTimeMillis();
            System.out.println("query Time: " + (end - start));
            System.out.println("Memory usage: " + RamUsageEstimator.sizeOf(stFilter) + " " +
                    RamUsageEstimator.humanSizeOf(stFilter));
            System.out.println(results.stream().mapToInt(List::size).sum());
        }
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
    public void testSTFilterIO() {
        int sBits = 8;
        int tBits = 4;
        AbstractSTFilter stFilter = new STFilter(sBits, tBits, new BasicFilterManager(3, 13));
//        insertIntoSTFilter(stFilter);
//        stFilter.out();
        List<List<byte[]>> results = shrinkBySTFilterWithIO(stFilter);
        checkNoFalsePositive(results);
    }

    @Test
    public void testStairBFIO() {
        StairBF bf = new StairBF(5,100, 20, 0, 1000);

        int n = 1000;
        for (int i = 0; i < n; ++i) {
            bf.insert(intToByte4(i), i);
        }
        for (int i = 0; i < n; ++i) {
            Assert.assertTrue(bf.query(intToByte4(i), i));
        }
        int err0 = 0;
        for (int i = n; i < n + n; ++i) {
            if (bf.query(intToByte4(i), i)) {
                ++err0;
            }
        }

        bf.out();
        bf.init();
        for (int i = n; i < n + n; ++i) {
            if (bf.query(intToByte4(i), i)) {
                --err0;
            }
        }
        Assert.assertEquals(err0, 0);
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
        for (Query query : QUERIES_SMALL) {
            List<byte[]> filterResult = filter.shrink(query, spatialKeyGenerator, timeKeyGenerator, keywordGenerator);
            results.add(filterResult);
        }
        return results;
    }

    private List<List<byte[]>> shrinkBySTFilter(ISTKFilter stFilter) throws IOException {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<byte[]> filterResult = stFilter.shrink(query);
            results.add(filterResult);
        }
        return results;
    }

    private List<List<byte[]>> shrinkBySTFilterWithIO(AbstractSTFilter stFilter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES_SMALL) {
            List<byte[]> filterResult = stFilter.shrinkWithIO(query);
            results.add(filterResult);
        }
        return results;
    }

    private List<List<byte[]>> shrinkBySTFilter(AbstractSTFilter stFilter, List<Query> queries) throws IOException {
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

    private static byte[] intToByte4(int i) {
        byte[] targets = new byte[4];
        targets[3] = (byte) (i & 0xFF);
        targets[2] = (byte) (i >> 8 & 0xFF);
        targets[1] = (byte) (i >> 16 & 0xFF);
        targets[0] = (byte) (i >> 24 & 0xFF);
        return targets;
    }
}
