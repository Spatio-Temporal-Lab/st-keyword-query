import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Assert;
import org.junit.Ignore;
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
    private final static int queryCountEachBin = 1000;
    private final static int timeBin = 4;   // 每timeBin小时生成一次查询，每次生成queryCountEachBin个查询
    private final static String dataDir = "D:\\data\\stkeyword\\tweetSorted.csv";   // 存储数据的文件，有序
    private final static String queryFileName = "streamTweet.csv";  //存储查询的文件
    private final static String queryDir = new File("").getAbsolutePath() + "/src/main/resources/" + queryFileName;
    private final static int sampleCount = 100_000;     //测试数据的量
    private final static String tableName = "testTweet";    // HBase存储数据的表名
    private final static String filterTableName = "tweetFilters";   // HBase存储布隆过滤器的表名
    private final static int sBits = 4;     //HBase键中空间键的后sBits位抹除，用于构建布隆过滤器的键，2的整数倍
    private final static int tBits = 2;     //HBase键中时间键的后tBits位抹除，用于构建布隆过滤器的键
    private final static int logInitFilterSlotSize = 3; // 布隆过滤器初始化槽的个数，log
    private final static int fingerSize = 13;           // 布隆过滤器初始化指纹长度

    private final HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

    @Test
    @Ignore
    public void generateQuery() throws IOException {
        Date win = null;
        StreamQueryGenerator queryGenerator = new StreamQueryGenerator(queryCountEachBin);

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
                    win = getHourDate(now);
                } else if (DateUtil.getHours(win, now) >= timeBin) {
                    List<Query> queries = queryGenerator.generatorQuery();
                    writeQueries(queries);
                    while (DateUtil.getHours(win, now) >= timeBin) {
                        win = DateUtil.getDateAfterHours(win, timeBin);
                    }
                }
                queryGenerator.insert(cur);
                if (++count >= sampleCount) {
                    break;
                }
            }
        }
    }

    @Test
    public void testSampleData() throws IOException, ParseException, InterruptedException {
        initFilterTable();
        StreamSTFilter filter = new StreamSTFilter(sBits, tBits, new StreamLRUFM(logInitFilterSlotSize, fingerSize, filterTableName));
        QueryProcessor processor = new QueryProcessor(tableName, filter);
        List<Query> queriesAll = getQueries();
        doVerify(filter, queriesAll, processor);
        processor.close();
    }

    private void doVerify(StreamSTFilter filter, List<Query> queriesAll,
                          QueryProcessor processor) throws IOException, ParseException, InterruptedException {
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
                    win = getHourDate(now);
                } else if (DateUtil.getHours(win, now) >= timeBin) {
                    filter.doClear();

                    if (i * queryCountEachBin >= queriesAll.size()) {
                        break;
                    }
                    List<Query> queries = queriesAll.subList(i * queryCountEachBin, (i + 1) * queryCountEachBin);
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

                    while (DateUtil.getHours(win, now) >= TestSTKQ.timeBin) {
                        win = DateUtil.getDateAfterHours(win, TestSTKQ.timeBin);
                    }
                }

                // put into filter
                filter.insert(cur);
                objects.add(cur);

                if (++count >= TestSTKQ.sampleCount) {
                    break;
                }
            }
        }
        System.out.println("time: " + (allTime / 100_0000) + "ms");
    }

    private void initFilterTable() throws IOException {
        if (!hBaseUtil.existsTable(filterTableName)) {
            hBaseUtil.createTable(filterTableName, "attr", BloomType.ROW);
        } else {
            hBaseUtil.truncateTable(filterTableName);
        }
    }

    /**
     * 根据输入的时间日期，获得只保留小时、日期的时间。
     * @param date 可能保留时间的日期
     * @return 只保留小时、日期的时间
     */
    private Date getHourDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    private void writeQueries(List<Query> queries) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(queryDir, true))) {
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

    private static List<Query> getQueries() throws IOException, ParseException {
        List<Query> queries = new ArrayList<>();
        try (InputStream in = QueryGenerator.class.getResourceAsStream("/" + queryFileName);
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
