package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.hbase.util.Bytes;
import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.filter.*;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataProcessor {
    private int limit;
    private double rate;
    private long ID;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSample.csv";
    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSampleBig.csv";

    String DELIMITER = ",";

    public DataProcessor() {
        limit = 10000_0000;
        rate = 1.0;
    }

    public DataProcessor(int limit, double rate) {
        this.limit = limit;
        this.rate = rate;
    }

    public int getLimit() {
        return limit;
    }

    public double getRate() {
        return rate;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public static boolean isNumeric(String str) {
        boolean flag = false;
        String tmp;
        if (str.length() > 0) {
            if (str.startsWith("-")) {
                tmp = str.substring(1);
            } else {
                tmp = str;
            }
            flag = tmp.matches("^[0.0-9.0]+$");
        }
        return flag;
    }

    public static boolean isAlphabet(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    public STObject getSTObject(String line) {
        if(new Random().nextDouble() > rate) {
            return null;
        }

        String[] columns = line.split(DELIMITER);

        int n = columns.length;
        if (n != 4) {
            return null;
        }

        double lat;
        double lon;
        if (isNumeric(columns[2]) && (isNumeric(columns[3]))) {
            lat = Double.parseDouble(columns[2]);
            lon = Double.parseDouble(columns[3]);
        } else {
            return null;
        }

        if (lat > 90 || lat < -90 || lon > 180 || lon < -180)
            return null;

        ArrayList<String> keywords = new ArrayList<>();

        String s = columns[1];
        int len = s.length();
        StringBuilder builder = new StringBuilder();
        for (int j = 0; j < len; ++j) {
            if (isAlphabet(s.charAt(j))) {
                builder.append(s.charAt(j));
            } else if (builder.length() != 0) {
                keywords.add(builder.toString());
                builder = new StringBuilder();
            }
        }
        if (builder.length() > 0) {
            keywords.add(builder.toString());
        }

        if (keywords.size() == 0) {
            return null;
        }

        Date date = null;
        try {
            String time = columns[0];
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new STObject(ID++, lat, lon, date, keywords);
    }

    public ArrayList<STObject> getSTObjects(String path) throws ParseException {

        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        Random random = new Random();

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();
        ArrayList<STObject> shops = new ArrayList<>(limit);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }
                if (random.nextDouble() > rate) {
                    continue;
                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
                shops.add(cur);

                if (shops.size() >= limit) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println("Dataset size: " + shops.size());

        return shops;
    }

    public BloomFilter<byte[]> generateBloomFilter(String path, int size, double p) throws ParseException {

        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), size, p);

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                long sID = spatialKeyGenerator.toNumber(cur.getLocation()) >>> (Constant.S_FILTER_ITEM_LEVEL << 1);
                int tID = timeKeyGenerator.toNumber(cur.getTime()) >>> Constant.T_FILTER_ITEM_LEVEL;

                for (String keyword : cur.getKeywords()) {
                    bloomFilter.put(ByteUtil.concat(Bytes.toBytes(keyword.hashCode()),
                            ByteUtil.getKByte(sID, 4),
                            ByteUtil.getKByte(tID, 3)
                            ));
                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);

        return bloomFilter;
    }

    public Map<BytesKey, ChainedInfiniFilter> generateSTDividedFilter(String path) throws ParseException {
        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        Map<BytesKey, ChainedInfiniFilter> map = new HashMap<>();

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        int tMin = Integer.MAX_VALUE;
        int tMax = -1;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                int tNow = timeKeyGenerator.toNumber(cur.getTime());
                tMin = Math.min(tNow, tMin);
                tMax = Math.max(tNow, tMax);

//                long sID = spatialKeyGenerator.toNumber(cur.getLocation()) >>> (Constant.S_FILTER_ITEM_LEVEL << 1);
//                int tID = timeKeyGenerator.toNumber(cur.getTime()) >>> Constant.T_FILTER_ITEM_LEVEL;
//
//                long sIDForBf = sID >>> ((Constant.FILTER_LEVEL - Constant.S_FILTER_ITEM_LEVEL) << 1);
//                int tIDForBf = tID >>> (Constant.FILTER_LEVEL - Constant.T_FILTER_ITEM_LEVEL);
//
//                int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
//                int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
//                BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, needByteCountForS), ByteUtil.getKByte(tIDForBf, needByteCountForT))));
//
//                ChainedInfiniFilter filter;
//                if (map.get(bfID) == null) {
//                    filter = new ChainedInfiniFilter(3, 10);
//                    filter.set_expand_autonomously(true);
//                    map.put(bfID, filter);
//                } else {
//                    filter = map.get(bfID);
//                }
//
//                for (String keyword : cur.getKeywords()) {
//                    byte[] insertValue = ByteUtil.concat(Bytes.toBytes(keyword.hashCode()), ByteUtil.getKByte(sID, 4), ByteUtil.getKByte(tID, 3));
//                    filter.insert(insertValue, false);
//                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("tMin = " + tMin);
        System.out.println("tMax = " + tMax);
        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);

        return map;
    }

    public void putFiltersToRedis(AbstractSTFilter stFilter, String path) throws ParseException, IOException {
        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                stFilter.insert(cur);

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

//        List<Query> queries = QueryGenerator.getQueries("queriesZipf.csv");
//        for (Query query : queries) {
//            query.setQueryType(QueryType.CONTAIN_ONE);
//            stFilter.shrink(query);
//        }
//        ((LRUSTFilter) stFilter).compress();

        System.out.println(stFilter.size());
        stFilter.out();
        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);
    }

    public void putFiltersToRedis(StairBF stFilter, String path) throws ParseException {
        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                stFilter.insert(cur);

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println(stFilter.size());
        stFilter.out();
        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);
    }

    public ChainedInfiniFilter generateOneFilter(String path) throws ParseException {
        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        Map<BytesKey, ChainedInfiniFilter> map = new HashMap<>();

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();


        ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 20);
        filter.set_expand_autonomously(true);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                long sID = spatialKeyGenerator.toNumber(cur.getLocation());
                int tID = timeKeyGenerator.toNumber(cur.getTime());

                for (String keyword : cur.getKeywords()) {
                    byte[] insertValue = ByteUtil.concat(Bytes.toBytes(keyword.hashCode()), ByteUtil.getKByte(sID, 4), ByteUtil.getKByte(tID, 3));
                    filter.insert(insertValue, false);
                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);

        return filter;
    }

    public Map<BytesKey, Long> generateCount(String path) throws ParseException {
        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        Map<BytesKey, Long> map = new HashMap<>();

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;

            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                long sID = spatialKeyGenerator.toNumber(cur.getLocation()) >>> (Constant.S_FILTER_ITEM_LEVEL << 1);
                int tID = timeKeyGenerator.toNumber(cur.getTime()) >>> Constant.T_FILTER_ITEM_LEVEL;

                long sIDForBf = sID >>> ((Constant.FILTER_LEVEL - Constant.S_FILTER_ITEM_LEVEL) << 1);
                int tIDForBf = tID >>> (Constant.FILTER_LEVEL - Constant.T_FILTER_ITEM_LEVEL);

                int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
                int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
                BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, needByteCountForS), ByteUtil.getKByte(tIDForBf, needByteCountForT))));

                map.merge(bfID, 1L, Long::sum);

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getTime().before(initFrom)) {
                    initFrom = cur.getTime();
                }
                if (cur.getTime().after(initEnd)) {
                    initEnd = cur.getTime();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);

        System.out.println(map);
        return map;
    }

    public ArrayList<Map> generateDistribution(String path) {
        Map<BytesKey, Integer> st2Count = new HashMap<>();
        Map<BytesKey, Set<String>> st2Keywords = new HashMap<>();

        ISpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();
        Random random = new Random();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }
                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }

                if (random.nextDouble() < 0.1) {
                    BytesKey bytesKey = new BytesKey(ByteUtil.concat(
                            spatialKeyGenerator.toBytes(cur.getLocation()),
                            timeKeyGenerator.toBytes(cur.getTime())
                    ));
                    st2Count.merge(bytesKey, 1, Integer::sum);
                    if (st2Keywords.containsKey(bytesKey)) {
                        st2Keywords.get(bytesKey).addAll(cur.getKeywords());
                    } else {
                        st2Keywords.put(bytesKey, new HashSet<>(cur.getKeywords()));
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        ArrayList<Map> result = new ArrayList<>();
        result.add(st2Count);
        result.add(st2Keywords);
        return result;
    }

    public Set<String> generateKeywords(String path) {
        Set<String> set = new TreeSet<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }
                STObject cur = getSTObject(line);
                if (cur == null) {
                    continue;
                }
                set.addAll(cur.getKeywords());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return set;
    }

    public static List<STObject> getSampleData() {
        List<STObject> objects = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(TWEET_SAMPLE_FILE))) {
            String line;
            int id = 0;
            while ((line = in.readLine()) != null) {
                STObject object = new STObject(line);
                object.setID(id++);
                objects.add(object);
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        return objects;
    }

    public static void main(String[] args) throws ParseException, IOException {
        DataProcessor dataProcessor = new DataProcessor();
        //t:[107376,113887]
        int sBits = 8;
        int tBits = 4;
        int tMin = 107376;
        int tMax = 113887;
//        AbstractSTFilter stFilter = new STFilter(3, 14, sBits, tBits);
//        AbstractSTFilter stFilter = new HSTFilter(3, 14, sBits, tBits);
//        AbstractSTFilter stFilter = new LRUSTFilter(3, 14, sBits, tBits);
//        dataProcessor.putFiltersToRedis(stFilter, "/usr/data/tweetAll.csv");
//        dataProcessor.putFiltersToRedis(stFilter, "/home/hadoop/data/tweetAll.csv");
//        dataProcessor.generateSTDividedFilter("E:\\data\\tweetAll.csv");

        StairBF bf = new StairBF(8, 42000, 20, tMin, tMax);
//        dataProcessor.putFiltersToRedis(bf, "/usr/data/tweetAll.csv");
        dataProcessor.putFiltersToRedis(bf, "/home/hadoop/data/tweetAll.csv");
        //792433880 755.7M
        //798493496 761.5M
    }
}
