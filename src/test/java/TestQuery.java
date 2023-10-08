import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.util.QueryGenerator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;

public class TestQuery {

    private static boolean equals_(ArrayList<STObject> a1, ArrayList<STObject> a2) {
        int n = a1.size();
        if (a2.size() != n) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            if (!a1.get(i).equals(a2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean equals(ArrayList<ArrayList<STObject>> a1, ArrayList<ArrayList<STObject>> a2) {
        int n = a1.size();
        if (a2.size() != n) {
            return false;
        }
        boolean f = true;
        for (int i = 0; i < n; ++i) {
            if (!equals_(a1.get(i), a2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {

        String tableName = "testTweet";
        String outPathName = Constant.DATA_DIR + "queryLog.txt";
        ArrayList<Query> queries = QueryGenerator.getQueries();


        long start;
        long end;
        start = System.currentTimeMillis();

        end = System.currentTimeMillis();
        System.out.println(end - start);


        QueryProcessor[] processors = new QueryProcessor[]{
        };

        ArrayList<ArrayList<ArrayList<STObject>>> results = new ArrayList<>(processors.length);
        for (int i = 0; i < processors.length; ++i) {
            results.add(new ArrayList<>());
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            System.out.println("query size: " + queries.size());

            for (int i = 0; i < processors.length; ++i) {
                long timeMethod = 0;
                for (Query query : queries) {
                    query.setQueryType(QueryType.CONTAIN_ONE);
                    long startTime = System.currentTimeMillis();
                    ArrayList<STObject> result = processors[i].getResult(query);
                    results.get(i).add(result);
                    long endTime = System.currentTimeMillis();
                    timeMethod += endTime - startTime;
                }
                System.out.println("method " + i + " time: " + timeMethod);
                System.out.println("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime());
                System.out.println("method " + i + " query bloom time: " + processors[i].getQueryBloomTime());
                System.out.println("origin size: " + processors[i].getAllSize());
                System.out.println("origin count: " + processors[i].getAllCount());

                writer.write("method " + i + " time: " + timeMethod + "\n");
                writer.write("method " + i + " query hbase time: " + processors[i].getQueryHBaseTime() + "\n");
                writer.write("method " + i + " query bloom time: " + processors[i].getQueryBloomTime() + "\n");
                writer.write("origin size: " + processors[i].getAllSize() + "\n");
                writer.write("origin count: " + processors[i].getAllCount() + "\n");

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (ArrayList<ArrayList<STObject>> result_ : results) {
            for (ArrayList<STObject> result : result_) {
                Collections.sort(result);
            }
        }

        for (int i = 0; i < processors.length; ++i) {
            for (int j = i + 1; j < processors.length; ++j) {
                if (!equals(results.get(i), results.get(j))) {
                    System.out.println(results);
                    System.out.println("result not equal: " + i + " " + j);
                }
                if (results.get(i).size() != results.get(j).size()) {
                    System.out.println("count not equal: " + i + " " + j);
                }
            }
        }

        HBaseQueryProcessor.close();
    }
}
