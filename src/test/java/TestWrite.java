import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.io.HBaseIO;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TSKeyGenerator;
import org.urbcomp.startdb.stkq.model.STObject;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;

public class TestWrite {
    public static void main(String[] args) throws ParseException, IOException {
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

        String inPathName = "/home/liruiyuan/tweet_2.csv";
        String outPathName = "/home/liruiyuan/log2.txt";
        long allTime = 0;

        String[] tableNames = new String[]{
                "testTimeFirst", "testSpatialFirst", "testHilbert", "testShard"
        };
//        AbstractSTKeyGenerator[] generators = new AbstractSTKeyGenerator[]{
//                new TimeFirstSTKeyGenerator(new SpatialKeyGenerator(), new TimeKeyGenerator()),
//                new SpatialFirstSTKeyGenerator(new SpatialKeyGenerator(), new TimeKeyGenerator()),
//                new SpatialFirstSTKeyGenerator(new HilbertSpatialKeyGenerator(), new TimeKeyGenerator()),
//                new ShardSTKeyGenerator(new SpatialKeyGenerator(), new TimeKeyGenerator())
//        };
        ISTKeyGenerator[] generators = new ISTKeyGenerator[]{
                new STKeyGenerator(),
                new TSKeyGenerator(),
        };
        DataProcessor dataProcessor = new DataProcessor();
        ArrayList<STObject> objects = new ArrayList<>(dataProcessor.getSTObjects(inPathName));

        int MAX_TEST_COUNT = 3;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            int n = tableNames.length;
            for (int testCount = 0; testCount < MAX_TEST_COUNT; ++testCount) {
                for (int i = 0; i < n; ++i) {
                    String tableName = tableNames[i];
                    hBaseUtil.truncateTable(tableName);
//                    AbstractSTKeyGenerator generator = generators[i];
//                    HBaseWriter hBaseWriter = new HBaseWriter(generator);
//                    hBaseWriter.putUnusedData(tableName, generator.getByteCount(), 100000);
                    ISTKeyGenerator generator = generators[i];
                    HBaseIO hBaseIO = new HBaseIO();
                    hBaseIO.putUnusedData(tableName, generator.getByteCount(), 100000);
                    long start = System.currentTimeMillis();
                    HBaseIO.putObjects(tableName, generator, objects, 5000);
                    long end = System.currentTimeMillis();
                    allTime += end - start;
                    System.out.println(tableName + " cost " + (end - start) + "ms");
                    writer.write(tableName + " cost " + (end - start) + "ms\n");
                }
            }
            writer.write("average time: " + (allTime / MAX_TEST_COUNT));
        }

        hBaseUtil.close();
    }
}
