import org.junit.Assert;
import org.junit.Test;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.filter.SetFilter;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
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

    @Test
    public void testInfiniFilterFPR() {
        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        List<STObject> objects = getSampleData();

        IFilter[] filters = new IFilter[]{
                new SetFilter(),
                new InfiniFilter()
        };

        long start = System.currentTimeMillis();
        for (STObject object : objects) {
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        ByteUtil.getKByte(s.hashCode(), 4),
                        spatialKeyGenerator.toKey(object.getLocation()),
                        timeKeyGenerator.toKey(object.getTime()));
                for (IFilter filter : filters) {
                    filter.insert(key);
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("insert time: " + (end - start));

        int[] sizes = new int[filters.length];
        List<List<List<byte[]>>> results = new ArrayList<>(filters.length);
        for (int i = 0; i < filters.length; ++i) {
            results.add(new ArrayList<>());
        }



        start = System.currentTimeMillis();
        for (Query query : QUERIES) {
            List<Range<Long>> spatialRanges = spatialKeyGenerator.toRanges(query);
            Range<Integer> timeRange = timeKeyGenerator.toRanges(query);
            List<String> keywords = query.getKeywords();

            int n = filters.length;
            for (int i = 0; i < n; ++i) {
                List<byte[]> filterResult = filters[i].filter(spatialRanges, timeRange, keywords, QUERY_TYPE);
                sizes[i] += filterResult.size();
                results.get(i).add(filterResult);
            }
        }


        // ensure no false negative
        List<List<byte[]>> trueResults = results.get(0);
        int queryLength = QUERIES.size();
        for (int i = 1; i < filters.length; ++i) {
            List<List<byte[]>> approximateResults = results.get(i);
            for (int j = 0; j < queryLength; ++j) {
                for (byte[] code : trueResults.get(j)) {
                    boolean find = false;
                    for (byte[] aCode : approximateResults.get(j)) {
                        if (Arrays.equals(aCode, code)) {
                            find = true;
                            break;
                        }
                    }
                    Assert.assertTrue(find);
                }
            }
        }
        end = System.currentTimeMillis();
        System.out.println("query time: " + (end - start));

        System.out.println(Arrays.toString(sizes));
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
