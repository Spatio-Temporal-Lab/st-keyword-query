package com.START.STKQ.exp;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.keyGenerator.*;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;
import com.START.STKQ.processor.QueryProcessor;
import com.START.STKQ.util.QueryGenerator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;

public class TestQuery {
    public static void main(String[] args) throws ParseException, InterruptedException {
        String[] tableNames = new String[] {
                "testTimeFirst", "testSpatialFirst", "testHilbert", "testShard"
        };

        String outPathName = "/home/liruiyuan/queryLog2.txt";
//        String outPathName = "E:\\data\\queryLog2.txt";

        ArrayList<Query> queries = QueryGenerator.getQueries();

//        SpatialTimeKeyGenerator stKeyGenerator = new SpatialTimeKeyGenerator();
//        SpatialTimeKeyGenerator stKeyGenerator1 = new SpatialTimeKeyGenerator(new HilbertSpatialKeyGenerator());
//        TimeSpatialKeyGenerator tsKeyGenerator = new TimeSpatialKeyGenerator();
//        stKeyGenerator.setFilterFlag(false);
//        stKeyGenerator1.setFilterFlag(false);
//        tsKeyGenerator.setFilterFlag(false);

        AbstractSTKeyGenerator[] objectKeyGenerators = new AbstractSTKeyGenerator[]{
                new TimeFirstSTKeyGenerator(),
                new SpatialFirstSTKeyGenerator(),
                new SpatialFirstSTKeyGenerator(new HilbertSpatialKeyGenerator(), new TimeKeyGenerator()),
                new ShardSTKeyGenerator()
        };

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            for (int i = 0; i < 4; ++i) {
                String tableName = tableNames[i];
                long timeMethod = 0;
                System.out.println("query size: " + queries.size());

                QueryProcessor processor = new QueryProcessor(tableName, objectKeyGenerators[i]);

                for (Query query : queries) {
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processor.getResult(query);
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println(tableName + " time: " + timeMethod);
                writer.write("our method's time: " + timeMethod + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
