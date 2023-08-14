import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.BaseFilter;
import org.urbcomp.startdb.stkq.filter.CIFilter;
import org.urbcomp.startdb.stkq.filter.SetFilter;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class testFPR {

    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSample.csv";

    static void generateSampleData() throws ParseException, IOException {
        DataProcessor processor = new DataProcessor();
        processor.setLimit(10_0000);
        ArrayList<STObject> objects = processor.getSTObjects(Constant.TWEET_DIR);

        try (BufferedWriter out = new BufferedWriter(new FileWriter(TWEET_SAMPLE_FILE))) {
            for (STObject object : objects) {
                out.write(object.toVSCLine() + '\n');
            }
        }
    }


    public static void main(String[] args) {

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        List<STObject> objects = getSampleData();

        BaseFilter[] filters = new BaseFilter[]{
                new SetFilter(),
                new CIFilter()
        };

        long start = System.currentTimeMillis();
        for (STObject object : objects) {
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        ByteUtil.getKByte(s.hashCode(), 4),
                        spatialKeyGenerator.toKey(object.getLocation()),
                        timeKeyGenerator.toKey(object.getTime()));
                for (BaseFilter filter : filters) {
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

        ArrayList<Query> queries = QueryGenerator.getQueries("queriesZipfSample.csv");

        start = System.currentTimeMillis();
        for (Query query : queries) {

            List<Range<Long>> spatialRanges = spatialKeyGenerator.toRanges(query);
            Range<Integer> timeRange = timeKeyGenerator.toRanges(query);
            ArrayList<String> keywords = query.getKeywords();
            QueryType queryType = QueryType.CONTAIN_ONE;

            int n = filters.length;
            for (int i = 0; i < n; ++i) {
                List<byte[]> filterResult = filters[i].filter(spatialRanges, timeRange, keywords, queryType);
                sizes[i] += filterResult.size();
                results.get(i).add(filterResult);
            }
        }


        // ensure no false negative
        List<List<byte[]>> trueResults = results.get(0);
        int queryLength = queries.size();
        for (int i = 1; i < filters.length; ++i) {
            List<List<byte[]>> approximateResults = results.get(i);
            for (int j = 0; j < queryLength; ++j) {
                boolean flag = true;
                for (byte[] code : trueResults.get(j)) {
                    boolean find = false;
                    for (byte[] aCode : approximateResults.get(j)) {
                        if (Arrays.equals(aCode, code)) {
                            find = true;
                            break;
                        }
                    }
                    if (!find) {
                        flag = false;
                        break;
                    }
                }
                if (!flag) {
                    System.err.println("error for query" + j);
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
