import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Assert;
import org.junit.Test;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.model.MBR;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.processor.QueryProcessor;
import org.urbcomp.startdb.stkq.stream.StreamLRUFM;
import org.urbcomp.startdb.stkq.stream.StreamQueryGenerator;
import org.urbcomp.startdb.stkq.stream.StreamSTFilter;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.urbcomp.startdb.stkq.util.QueryGenerator;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.*;

public class TestSTKQ {
    private final int QUERY_COUNT_EACH_TIME_BIN = 1000;

    private final HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

    /**
     * @param dataDir The address of the data file
     * @param outDir The address to which the generated query is written
     */
    void generateQuery(String dataDir, String outDir, int timeBin, int dataCount) {
        Date win = null;
        StreamQueryGenerator queryGenerator = new StreamQueryGenerator(QUERY_COUNT_EACH_TIME_BIN);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(dataDir).toPath())))) {
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
                } else if (DateUtil.getHours(win, now) >= timeBin) {
                    List<Query> queries = queryGenerator.generatorQuery();
                    writeQueries(queries, outDir);
                    while (DateUtil.getHours(win, now) >= timeBin) {
                        win = DateUtil.getDateAfterHours(win, timeBin);
                    }
                }
                queryGenerator.insert(cur);
                if (++count >= dataCount) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testSampleData() throws IOException {
        // just need to run once
//        String queryFileName = "streamTweet";
//        String queryDir  = new File("").getAbsolutePath() + "/src/main/resources/" + queryFileName;
//        generateQuery("E:\\data\\tweetSorted.csv", queryDir, 4, 100000);
        testTweetSample(4, 2, 3, 13, 4, 100000);
    }

    public void testTweetSample(int sBits, int tBits, int log2Size, int bitsPerKey, int timeBin, int sampleCount) throws IOException {
        String dataDir = "E:\\data\\tweetSorted.csv";
        String queryFileName = "streamTweet.csv";
        String tableName = "testTweet";
        String filterTableName = "tweetFilters";

        initFilterTable(filterTableName);

        StreamSTFilter filter = new StreamSTFilter(sBits, tBits, new StreamLRUFM(log2Size, bitsPerKey, filterTableName));
        QueryProcessor processor = new QueryProcessor(tableName, filter);

        List<Query> queriesAll = getQueries(queryFileName);

        doVerify(dataDir, timeBin, filter, queriesAll, processor, sampleCount);
    }

    private void doVerify(String dataDir, int timeBin, StreamSTFilter filter, List<Query> queriesAll,
                          QueryProcessor processor, int sampleCount) {
        long allTime = 0;
        int i = 0;
        Date win = null;
        List<STObject> objects = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(dataDir).toPath())))) {
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
                } else if (DateUtil.getHours(win, now) >= timeBin) {
                    filter.doClear();

                    if (i * QUERY_COUNT_EACH_TIME_BIN >= queriesAll.size()) {
                        break;
                    }
                    List<Query> queries = queriesAll.subList(i * QUERY_COUNT_EACH_TIME_BIN, (i + 1) * QUERY_COUNT_EACH_TIME_BIN);
                    ++i;

                    long begin = System.nanoTime();
                    for (Query query : queries) {
                        List<STObject> results = processor.getResult(query);
                        Collections.sort(results);

                        List<STObject> results1 = bruteForce(objects, query);
                        Collections.sort(results1);

                        if (!equals(results, results1)) {
                            System.out.println(query);
                            System.out.println(results);
                            System.out.println(results1);
                            Assert.fail();
                        }

                    }
                    allTime += System.nanoTime() - begin;

                    while (DateUtil.getHours(win, now) >= timeBin) {
                        win = DateUtil.getDateAfterHours(win, timeBin);
                    }
                }

                // put into filter
                filter.insert(cur);
                objects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ParseException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("time: " + (allTime / 100_0000) + "ms");
        processor.close();
    }

    private void initFilterTable(String tableName) throws IOException {
        if (!hBaseUtil.existsTable(tableName)) {
            hBaseUtil.createTable(tableName, "attr", BloomType.ROW);
        } else {
            hBaseUtil.truncateTable(tableName);
        }
    }

    private Date getDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    private void writeQueries(List<Query> queries, String file) throws IOException {
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

                List<String> keywords = query.getKeywords();
                for (String keyword : keywords) {
                    writer.write("," + keyword);
                }
                writer.newLine();
            }
        }
    }

    private static List<Query> getQueries(String fileName) {
        List<Query> queries = new ArrayList<>();
        try (InputStream in = QueryGenerator.class.getResourceAsStream("/" + fileName);
             BufferedReader br = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)))) {
            String DELIMITER = ",";
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

    private List<STObject> bruteForce(List<STObject> objects, Query query) {
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
