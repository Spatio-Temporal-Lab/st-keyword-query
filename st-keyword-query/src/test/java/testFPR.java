import com.START.STKQ.constant.Constant;
import com.START.STKQ.constant.QueryType;
import com.START.STKQ.filter.BaseFilter;
import com.START.STKQ.filter.CIFilter;
import com.START.STKQ.filter.SetFilter;
import com.START.STKQ.io.DataProcessor;
import com.START.STKQ.keyGenerator.HilbertSpatialKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialKeyGenerator;
import com.START.STKQ.keyGenerator.TimeKeyGenerator;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.QueryGenerator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

public class testFPR {

    static void generateSampleData() throws ParseException, IOException {
        DataProcessor processor = new DataProcessor();
        processor.setLimit(10_0000);
        ArrayList<STObject> objects = processor.getSTObjects(Constant.TWEET_DIR);
        System.out.println(objects.size());


        String path = "src/main/resources/tweetSample.txt";
        try(OutputStream os = Files.newOutputStream(Paths.get(path));
            ObjectOutputStream oos = new ObjectOutputStream(os)) {
            oos.writeObject(objects);
        }
    }

    static ArrayList<STObject> getSampleData() {
        String path = "src/main/resources/tweetSample.txt";
        try(InputStream is = Files.newInputStream(Paths.get(path));
            ObjectInputStream ois = new ObjectInputStream(is)) {
            return (ArrayList<STObject>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        ArrayList<STObject> objects = getSampleData();

        BaseFilter[] filters = new BaseFilter[]{
                new SetFilter(),
                new CIFilter()
        };

        for (STObject object : objects) {
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        ByteUtil.getKByte(s.hashCode(), 4),
                        spatialKeyGenerator.toKey(object.getLocation()),
                        timeKeyGenerator.toKey(object.getDate()));
                for (BaseFilter filter : filters) {
                    filter.insert(key);
                }
            }
        }

        int[] sizes = new int[filters.length];
        ArrayList<ArrayList<ArrayList<byte[]>>> results = new ArrayList<>(filters.length);
        for (int i = 0; i < filters.length; ++i) {
            results.add(new ArrayList<>());
        }

        ArrayList<Query> queries = QueryGenerator.getQueries("queriesZipfSample.csv");
        for (Query query : queries) {

            ArrayList<Range<Long>> spatialRanges = spatialKeyGenerator.toRanges(query);
            Range<Integer> timeRange = timeKeyGenerator.toRanges(query);
            ArrayList<String> keywords = query.getKeywords();
            QueryType queryType = QueryType.CONTAIN_ONE;

            int n = filters.length;
            for (int i = 0; i < n; ++i) {
                ArrayList<byte[]> filterResult = filters[i].filter(spatialRanges, timeRange, keywords, queryType);
                sizes[i] += filterResult.size();
                results.get(i).add(filterResult);
            }
        }


        // ensure no false negative
        ArrayList<ArrayList<byte[]>> trueResults = results.get(0);
        int queryLength = queries.size();
        for (int i = 1; i < filters.length; ++i) {
            ArrayList<ArrayList<byte[]>> approximateResults = results.get(i);
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


        System.out.println(Arrays.toString(sizes));
    }
}
