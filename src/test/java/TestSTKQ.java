import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;
import org.urbcomp.startdb.stkq.STILT.STILTIndex;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.filter.STKFilter;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.io.LevelDbIO;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.model.MBR;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.preProcessing.DataProcessor;
import org.urbcomp.startdb.stkq.processor.*;
import org.urbcomp.startdb.stkq.stream.*;
import org.urbcomp.startdb.stkq.util.DateUtil;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.*;
import java.text.ParseException;
import java.util.*;

public class TestSTKQ {
    // 查询生成相关配置
    private final static int queryCountEachBin = 1000;
    private final static int timeBin = 4;   // 每timeBin小时生成一次查询，每次生成queryCountEachBin个查询
    private final static String dataDir = "D:\\data\\tweetSorted.csv";   // 存储数据的文件，有序
    private final static String queryDir = new File("").getAbsolutePath() + "/src/main/resources/streamTweet.csv";
    private final static int sampleCount = 100_000;     //测试数据的量
    private final static String tableName = "testTweet";    // HBase存储数据的表名
    private final static String filterTableName = "tweetFilters_Ruiyuan";   // HBase存储布隆过滤器的表名

    // 布隆过滤器相关配置
    private final static int sBits = 4;     //HBase键中空间键的后sBits位抹除，用于构建布隆过滤器的键，2的整数倍
    private final static int tBits = 2;     //HBase键中时间键的后tBits位抹除，用于构建布隆过滤器的键
    private final static int logInitFilterSlotSize = 3; // 布隆过滤器初始化槽的个数，log
    private final static int fingerSize = 15;           // 布隆过滤器初始化指纹长度

    // 替换相关配置
    private final static long maxRamUsage = 10 * 1024 * 1024;  // 布隆过滤器最大占用内存

    private final HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

    @Test
    public void generateQuery() throws IOException {
        Date window = null;
        StreamQueryGenerator queryGenerator = new StreamQueryGenerator(queryCountEachBin,
            QueryDistributionEnum.LINEAR, QueryType.CONTAIN_ONE);

        try (BufferedReader br = new BufferedReader(new FileReader(dataDir));
             BufferedWriter writer = new BufferedWriter(new FileWriter(queryDir, false))
        ) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {
                    List<Query> queries = queryGenerator.generateQuery();
                    writeQueries(queries, writer);
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
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
//        initFilterTable();
        LevelDbIO.clearData();

        Date window = null;
        List<Query> queriesAll = getQueries();
        StreamSTKFilter filter = new StreamSTKFilter(sBits, tBits,
                new StreamLRUFilterManager(logInitFilterSlotSize, fingerSize, filterTableName, maxRamUsage));
        List<STObject> totalObjects = new ArrayList<>();
        
        int timeBinId = 0;
        long allTime = 0;
        try (QueryProcessor processor = new QueryProcessor(tableName, filter);
             BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;
            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {
                    // 即将创建新的timeBin，尝试调整过滤器
                    filter.doClearAfterBatchInsertion();

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = processor.getResult(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                // 插入过滤器
                filter.insert(cur);
                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
        }
    }

    @Test
    public void testSampleDataRedis() throws IOException, ParseException {
        Date window = null;

        RedisIO.flush();
        List<Query> queriesAll = getQueries();
//        StreamLRUFilterManager filterManager =
//                new StreamLRUFilterManager(logInitFilterSlotSize, fingerSize, filterTableName, maxRamUsage);
//        StreamSTKFilter filter = new StreamSTKFilter(sBits, tBits, filterManager);
        STKFilter filter = new STKFilter(sBits, tBits, new BasicFilterManager(logInitFilterSlotSize, fingerSize));
        List<STObject> totalObjects = new ArrayList<>();

        STKeyGenerator keyGenerator = new STKeyGenerator();

        int timeBinId = 0;
        long allTime = 0;

        try (RedisReader reader = new RedisReader(filter);
                BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;

            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {
//                    filterManager.doClearAfterBatchInsertion();

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = reader.scan(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                // 插入过滤器
                filter.insert(cur);
                // 插入redis
                RedisIO.zAdd(0, new byte[]{0}, keyGenerator.toNumber(cur), cur.toByteArray());
                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
            System.out.println("Ram usage: " + RamUsageEstimator.humanSizeOf(filter));
        }
    }

    @Test
    public void testSampleDataRedisWithoutFilter() throws IOException, ParseException {
        Date window = null;

        RedisIO.flush();
        List<Query> queriesAll = getQueries();
        List<STObject> totalObjects = new ArrayList<>();

        STKeyGenerator keyGenerator = new STKeyGenerator();

        int timeBinId = 0;
        long allTime = 0;

        try (BasicRedisReader reader = new BasicRedisReader(keyGenerator);
             BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;

            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = reader.scan(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                // 插入redis
                RedisIO.zAdd(0, new byte[]{0}, keyGenerator.toNumber(cur), cur.toByteArray());
                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
        }
    }

    @Test
    public void testSampleDataSTILT() throws IOException, ParseException {
        Date window = null;

        RedisIO.flush();
        List<Query> queriesAll = getQueries();
        List<STObject> totalObjects = new ArrayList<>();

        int timeBinId = 0;
        long allTime = 0;
        long id = 0;

        try (STILTIndex index = new STILTIndex((byte) 64);
             BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;

            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = index.getResult(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                cur.setID(id++);
                index.insert(cur);
                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
            System.out.println("Ram usage: " + RamUsageEstimator.humanSizeOf(index));
        }
    }

    @Test
    public void testSampleDataStair() throws IOException, ParseException, InterruptedException {
        LevelDbIO.clearData();

        Date window = null;
        List<Query> queriesAll = getQueries();
        StairStreamSTFilter filter = new StairStreamSTFilter(sBits, tBits,
                new StairStreamLRUFilterManager(logInitFilterSlotSize, fingerSize, new long[]{
                        maxRamUsage * 3 / 5, maxRamUsage / 5, maxRamUsage / 5
                }));
        List<STObject> totalObjects = new ArrayList<>();

        int timeBinId = 0;
        long allTime = 0;
        try (QueryProcessor processor = new QueryProcessor(tableName, filter);
             BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;
            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {
                    // 即将创建新的timeBin，尝试调整过滤器
                    filter.doClear();

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = processor.getResult(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                // 插入过滤器
                filter.insert(cur);
                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
        }
    }

    @Test
    public void testSampleDataWithoutFilter() throws IOException, ParseException, InterruptedException {
        Date window = null;
        List<Query> queriesAll = getQueries();
        List<STObject> totalObjects = new ArrayList<>();

        int timeBinId = 0;
        long allTime = 0;
        try (AbstractQueryProcessor processor = new BasicQueryProcessor(tableName, new STKeyGenerator());
             BufferedReader dataBr = new BufferedReader(new FileReader(dataDir))) {

            String line;
            int count = 0;
            while ((line = dataBr.readLine()) != null) {
                STObject cur = DataProcessor.parseSTObject(line);
                if (cur == null) {
                    continue;
                }
                Date now = cur.getTime();
                if (window == null) {
                    window = getHourDate(now);
                } else if (DateUtil.getHours(window, now) >= timeBin) {

                    // 获取当前timeBin的查询
                    List<Query> queries = queriesAll.subList(timeBinId * queryCountEachBin, (timeBinId + 1) * queryCountEachBin);
                    ++timeBinId;

                    // 执行查询
                    for (Query query : queries) {
                        //获取查询结果
                        long begin = System.nanoTime();
                        List<STObject> queryResults = processor.getResult(query);
                        allTime += System.nanoTime() - begin;

                        //获取groundTruth
                        List<STObject> groundTruth = getGroundTruthByQuery(query, totalObjects);

                        //校验
                        if (!equals(queryResults, groundTruth)) {
                            System.out.println("query: " + query);
                            System.out.println("queryResult:" + queryResults);
                            System.out.println("groundTruth:" + groundTruth);
                            Assert.fail();
                        }
                    }

                    // 找到下一个timeBin
                    while (DateUtil.getHours(window, now) >= timeBin) {
                        window = DateUtil.getDateAfterHours(window, timeBin);
                    }
                }

                // 保存在数组中，用于校验结果
                totalObjects.add(cur);

                if (++count >= sampleCount) {
                    break;
                }
            }

            int queryCount = queriesAll.size();
            System.out.println("QueryCount: " + queryCount);
            System.out.println("Avg Time: " + (allTime * 1.0 / queryCount / 1000_000) + "ms");
        }
    }

    private List<STObject> getGroundTruthByQuery(Query query, List<STObject> totalObjects) {
        List<STObject> groundTruth = new ArrayList<>();
        for (int i = totalObjects.size() - 1; i >= 0; i--) {
            STObject object = totalObjects.get(i);
            if (STKUtil.check(object, query)) {
                groundTruth.add(object);
            }
            if (object.getTime().before(query.getStartTime())) {
                break;
            }
        }
        return groundTruth;
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
     *
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

    private void writeQueries(List<Query> queries, BufferedWriter writer) throws IOException {
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

    private static List<Query> getQueries() throws IOException, ParseException {
        List<Query> queries = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(queryDir))) {
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
                List<String> keywords = new ArrayList<>(Arrays.asList(array).subList(6, array.length));
                queries.add(new Query(lat1, lat2, lon1, lon2, s, t, keywords));
            }
        }
        return queries;
    }

    private static boolean equals(List<STObject> a1, List<STObject> a2) {
        int n = a1.size();
        if (a2.size() != n) {
            return false;
        }
        Collections.sort(a1);
        Collections.sort(a2);
        for (int i = 0; i < n; ++i) {
            if (!a1.get(i).equals(a2.get(i))) {
                return false;
            }
        }
        return true;
    }
}
