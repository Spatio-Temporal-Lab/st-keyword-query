package org.urbcomp.startdb.stkq.stream;

import org.apache.hadoop.hbase.regionserver.BloomType;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.MBR;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.urbcomp.startdb.stkq.util.GeoUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.*;

public class StreamSTKQuery {
    private final static String DIR = "/usr/data/tweetSorted.csv";
    private final static int TIME_BIN = 4;

    private static Date getDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    public static void main(String[] args) throws IOException {
//        RedisIO.flush(0);
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
        String tableName = "filters";
        if (!hBaseUtil.existsTable(tableName)) {
            hBaseUtil.createTable(tableName, "attr", BloomType.ROW);
        } else {
            hBaseUtil.truncateTable(tableName);
        }

        Date win = null;
        StreamSTFilter filter = new StreamSTFilter(4, 2, new StreamLRUFM(3, 13));
//        StreamSTFilter filter = new StreamSTFilter(4, 2, new AStreamLRUFM(3, 13));
        StreamQueryGenerator queryGenerator = new StreamQueryGenerator();
        QueryProcessor processor = new QueryProcessor("testTweet", filter);
        List<STObject> objects = new ArrayList<>();

        List<Query> queriesAll = getQueries("streamTweet.csv");

        long allTime = 0;

        int i = 0;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(DIR).toPath())))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();

                if (win == null) {
                    win = getDate(now);
                } else if (DateUtil.getHours(win, now) >= TIME_BIN) {

                    filter.doClear();
//                    List<Query> queries = queryGenerator.generatorQuery();

                    if (i * 1000 >= queriesAll.size()) {
                        break;
                    }
                    List<Query> queries = queriesAll.subList(i * 1000, (i + 1) * 1000);
                    ++i;

//                    String path = new File("").getAbsolutePath() + "/src/main/resources/streamTweet.csv";
//                    writeQueries(queries, path);

                    System.out.println(win);

                    long begin = System.nanoTime();
                    for (Query query : queries) {
                        List<STObject> results = processor.getResult(query);

//                        Collections.sort(results);
//                        List<STObject> results1 = bruteForce(objects, query);
//                        Collections.sort(results1);
//                        if (!equals(results, results1)) {
//                            System.out.println("error for " + query);
//                            System.out.println("query results = " + results);
//                            System.out.println("brute force = " + results1);
//                            System.out.println("--------------------------------");
//                        }
                    }
                    allTime += System.nanoTime() - begin;

                    while (DateUtil.getHours(win, now) >= TIME_BIN) {
                        win = DateUtil.getDateAfterHours(win, TIME_BIN);
                    }
                }
//                System.out.println(win + " " + now);

                // put into filter
                filter.insert(cur);
                queryGenerator.insert(cur);
//                objects.add(cur);

                if (++count >= 100000) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ParseException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println(allTime / 100_0000);
//        System.out.println(processor.getQueryHBaseTime());
        processor.close();
//        RedisIO.close();
    }

    private static void writeQueries(List<Query> queries, String file) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            for (Query query : queries) {
                MBR mbr = query.getMBR();
                writer.write(mbr.getMinLat() + "," + mbr.getMaxLat());
                writer.write(",");
                writer.write(mbr.getMinLon() + "," + mbr.getMaxLon());
                writer.write(",");

                writer.write(DateUtil.format(query.getStartTime()));
                writer.write(",");
                writer.write(DateUtil.format(query.getEndTime()));

                ArrayList<String> keywords = query.getKeywords();
                for (String keyword : keywords) {
                    writer.write("," + keyword);
                }
                writer.newLine();
            }
        }
    }

    private static ArrayList<Query> getQueries(String fileName) {
        ArrayList<Query> queries = new ArrayList<>();
        try (InputStream in = QueryGenerator.class.getResourceAsStream("/" + fileName);
             BufferedReader br = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
            // CSV文件的分隔符
            String DELIMITER = ",";
            // 按行读取
            String line;
            while ((line = br.readLine()) != null) {
                String[] array = line.split(DELIMITER);
                double lat1 = Double.parseDouble(array[0]);
                double lat2 = Double.parseDouble(array[1]);
                double lon1 = Double.parseDouble(array[2]);
                double lon2 = Double.parseDouble(array[3]);
                Date s = DateUtil.getDate(array[4]);
                Date t = DateUtil.getDate(array[5]);
                ArrayList<String> keywords = new ArrayList<>(Arrays.asList(array).subList(6, array.length));
                queries.add(new Query(lat1, lat2, lon1, lon2, s, t, keywords));
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        return queries;
    }

    static List<STObject> bruteForce(List<STObject> objects, Query query) {
        List<STObject> results = new ArrayList<>();
        for (STObject object : objects) {
            if (STKUtil.check(object, query)) {
                results.add(object);
            }
        }
        return results;
    }

    private static boolean equals(List<STObject> a1, List<STObject> a2) {
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
}
