import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.SetFilter;
import org.urbcomp.startdb.stkq.keyGenerator.old.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestFilters {

    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSample.csv";
    private static final QueryType QUERY_TYPE = QueryType.CONTAIN_ONE;
    private static final List<Query> QUERIES = QueryGenerator.getQueries("queriesZipfSample.csv");
    private static final List<STObject> SAMPLE_DATA = getSampleData();
    private static final SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
    private static final TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();
    private static List<List<byte[]>> GROUND_TRUTH_RANGES = new ArrayList<>();

    @BeforeClass
    public static void initial() {
        IFilter setFilter = new SetFilter();
        insertIntoFilter(setFilter);
        GROUND_TRUTH_RANGES = shrinkByFilter(setFilter);
        assertEquals(13365, GROUND_TRUTH_RANGES.stream().mapToInt(List::size).sum());
    }

    @Test
    public void testInfiniFilterFPR() {
        IFilter filter = new InfiniFilter();

        long start = System.currentTimeMillis();
        insertIntoFilter(filter);
        long end = System.currentTimeMillis();
        System.out.println("Insert Time: " + (end - start));

        start = System.currentTimeMillis();
        List<List<byte[]>> results = shrinkByFilter(filter);
        end = System.currentTimeMillis();
        System.out.println("Query Time: " + (end - start));
        System.out.println("Result Size: " + results.stream().mapToInt(List::size).sum());

        checkNoFalsePositive(results);
    }

    private static void insertIntoFilter(IFilter filter) {
        for (STObject object : SAMPLE_DATA) {
            byte[] spatialKey = spatialKeyGenerator.toKey(object.getLocation());
            byte[] timeKey = timeKeyGenerator.toKey(object.getTime());
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        ByteUtil.getKByte(s.hashCode(), Constant.KEYWORD_BYTE_COUNT),
                        spatialKey,
                        timeKey);
                filter.insert(key);
            }
        }
    }

    private static List<List<byte[]>> shrinkByFilter(IFilter filter) {
        List<List<byte[]>> results = new ArrayList<>();
        for (Query query : QUERIES) {
            List<Range<Long>> spatialRanges = spatialKeyGenerator.toRanges(query);
            Range<Integer> timeRange = timeKeyGenerator.toRanges(query);
            List<String> keywords = query.getKeywords();

            List<byte[]> filterResult = filter.shrink(spatialRanges, timeRange, keywords, QUERY_TYPE);
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

    private static List<STObject> getSampleData() {
        List<STObject> objects = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(TWEET_SAMPLE_FILE))) {
            String line;
            while ((line = in.readLine()) != null) {
                objects.add(new STObject(line));
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        return objects;
    }
}
