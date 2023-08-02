import com.START.STKQ.constant.Constant;
import com.START.STKQ.io.DataProcessor;
import com.START.STKQ.keyGenerator.*;
import com.START.STKQ.model.BytesKey;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.QueryGenerator;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import scala.util.control.Exception;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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
        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 12);
        filter.set_expand_autonomously(true);

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        ArrayList<STObject> objects = getSampleData();
        Set<BytesKey> set = new HashSet<>();

        for (STObject object : objects) {
            for (String s : object.getKeywords()) {
                byte[] key = ByteUtil.concat(
                        ByteUtil.getKByte(s.hashCode(), 4),
                        spatialKeyGenerator.toKey(object.getLocation()),
                        timeKeyGenerator.toKey(object.getDate()));
                filter.insert(key, false);
                set.add(new BytesKey(key));
            }
        }

        int exact = 0;
        int real = 0;

        ArrayList<Query> queries = QueryGenerator.getQueries("queriesZipfSample.csv");
        for (Query query : queries) {

            Range<Integer> timeRange = timeKeyGenerator.toRanges(query);
            ArrayList<Range<Long>> spatialRanges = spatialKeyGenerator.toRanges(query);

            int tStart = timeRange.getLow();
            int tEnd = timeRange.getHigh();
            for (Range<Long> spatialRange : spatialRanges) {
                long spatialRangeStart = spatialRange.getLow();
                long spatialRangeEnd = spatialRange.getHigh();

                for (long i = spatialRangeStart; i <= spatialRangeEnd; ++i) {
                    for (int j = tStart; j <= tEnd; ++j) {

                        boolean containOneForFilter = false;
                        boolean containOne = false;

                        for (String s : query.getKeywords()) {
                            byte[] key = ByteUtil.concat(
                                    ByteUtil.getKByte(s.hashCode(), 4),
                                    ByteUtil.getKByte(i, 4),
                                    ByteUtil.getKByte(j, 3)
                                    );
                            if (filter.search(key)) {
                                containOneForFilter = true;
                            }
                            if (set.contains(new BytesKey(key))) {
                                containOne = true;
                            }
                        }

                        if (containOne) ++exact;
                        if (containOneForFilter) ++real;
                    }
                }
            }

        }

        System.out.println("exact = " +  exact);
        System.out.println("real = " + real);
        System.out.println("filter size = " + RamUsageEstimator.humanSizeOf(filter));
        System.out.println("set size = " + RamUsageEstimator.humanSizeOf(set));
    }
}
