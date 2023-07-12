package com.START.STKQ.io;

import com.START.STKQ.constant.Constant;
import com.START.STKQ.keyGenerator.*;
import com.START.STKQ.model.*;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import com.START.STKQ.util.KeywordCounter;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import com.github.nivdayan.FilterLibrary.filters.Filter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.geomesa.curve.NormalizedDimension;
import scala.collection.immutable.Stream;

import java.io.*;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataReader {
    private int limit;
    private double rate;
    private Range<Location> locationRange;
    private Range<Date> timeRange;
    private long ID;
    private BufferedReader br;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String DELIMITER = ",";

    public DataReader() throws ParseException {
        limit = 10000_0000;
        rate = 1.0;
        locationRange = new Range<>(new Location(-90, -180), new Location(90, 180));
        timeRange = new Range<>(DateUtil.getDate("2000-01-01 00:00:00"), DateUtil.getDate("2023-12-31 23:59:59"));
    }

    public DataReader(int limit, double rate) throws ParseException {
        this.limit = limit;
        this.rate = rate;
        locationRange = new Range<>(new Location(-90, -180), new Location(90, 180));
        timeRange = new Range<>(DateUtil.getDate("2000-01-01 00:00:00"), DateUtil.getDate("2023-12:31 23:59:59"));
    }

    public DataReader(int limit, double rate, String path) throws ParseException {
        this.limit = limit;
        this.rate = rate;
        locationRange = new Range<>(new Location(-90, -180), new Location(90, 180));
        timeRange = new Range<>(DateUtil.getDate("2000-01-01 00:00:00"), DateUtil.getDate("2023-12:31 23:59:59"));

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();
        ArrayList<STObject> shops = new ArrayList<>(limit);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            this.br = br;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }


    public int getLimit() {
        return limit;
    }

    public double getRate() {
        return rate;
    }

    public Range<Location> getLocationRange() {
        return locationRange;
    }

    public void setLocationRange(Range<Location> locationRange) {
        this.locationRange = locationRange;
    }

    public Range<Date> getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(Range<Date> timeRange) {
        this.timeRange = timeRange;
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

        Location locationLow = locationRange.getLow();
        Location locationHigh = locationRange.getHigh();
        double lat1 = locationLow.getLat();
        double lat2 = locationHigh.getLat();
        double lon1 = locationLow.getLon();
        double lon2 = locationHigh.getLon();

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
        if (lat < lat1 || lat > lat2 || lon < lon1 || lon > lon2)
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

        for (String keyword : keywords) {
            KeywordCounter.add(keyword);
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
                if (cur.getDate().before(initFrom)) {
                    initFrom = cur.getDate();
                }
                if (cur.getDate().after(initEnd)) {
                    initEnd = cur.getDate();
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

    public void getSTObjectsBySpatial(String path) throws ParseException {
        //实现对象读取

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            int[][] count = new int[32][32];
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

                count[(int) ((cur.getLat() + 90) / 5.625)][(int) ((cur.getLon() + 180) / 11.25)] += 1;
            }

            for (int[] ints : count) {
                System.out.println(Arrays.toString(ints));
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
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

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
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
                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> 4;
                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> 2;
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
                if (cur.getDate().before(initFrom)) {
                    initFrom = cur.getDate();
                }
                if (cur.getDate().after(initEnd)) {
                    initEnd = cur.getDate();
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

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
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
//                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> 4;
//                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> 2;
                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> Constant.FILTER_ITEM_LEVEL;

//                long sIDForBf = sID >>> 12;
//                int tIDForBf = tID >>> 6;

//                long sIDForBf = sID >>> 16;
//                int tIDForBf = tID >>> 8;
                long sIDForBf = sID >>> ((Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL) << 1);
                int tIDForBf = tID >>> (Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL);

                int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
                int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
                BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, needByteCountForS), ByteUtil.getKByte(tIDForBf, needByteCountForT))));

                ChainedInfiniFilter filter;
                if (map.get(bfID) == null) {
                    filter = new ChainedInfiniFilter(3, 10);
                    filter.set_expand_autonomously(true);
                    map.put(bfID, filter);
                } else {
                    filter = map.get(bfID);
                }

                for (String keyword : cur.getKeywords()) {
                    byte[] insertValue = ByteUtil.concat(Bytes.toBytes(keyword.hashCode()), ByteUtil.getKByte(sID, 4), ByteUtil.getKByte(tID, 3));
                    filter.insert(insertValue, false);
                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getDate().before(initFrom)) {
                    initFrom = cur.getDate();
                }
                if (cur.getDate().after(initEnd)) {
                    initEnd = cur.getDate();
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

        return map;
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

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
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
//                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> 4;
//                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> 2;
                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> (Constant.FILTER_ITEM_LEVEL << 1);
                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> Constant.FILTER_ITEM_LEVEL;

//                long sIDForBf = sID >>> 16;
//                int tIDForBf = tID >>> 8;
                long sIDForBf = sID >>> ((Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL) << 1);
                int tIDForBf = tID >>> (Constant.FILTER_LEVEL - Constant.FILTER_ITEM_LEVEL);

                int needByteCountForS = Constant.SPATIAL_BYTE_COUNT - Constant.FILTER_LEVEL / 4;
                int needByteCountForT = Constant.TIME_BYTE_COUNT - Constant.FILTER_LEVEL / 8;
                BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, needByteCountForS), ByteUtil.getKByte(tIDForBf, needByteCountForT))));
//                BytesKey bfID = new BytesKey(ByteUtil.concat(ByteUtil.concat(ByteUtil.getKByte(sIDForBf, 2), ByteUtil.getKByte(tIDForBf, 2))));

                map.merge(bfID, 1L, Long::sum);

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getDate().before(initFrom)) {
                    initFrom = cur.getDate();
                }
                if (cur.getDate().after(initEnd)) {
                    initEnd = cur.getDate();
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

    public ArrayList<BloomFilter<byte[]>> generateBloomFilters(String path, int size, double p) throws ParseException {

        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        ArrayList<BloomFilter<byte[]>> bloomFilters = new ArrayList<>();
        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), size, p);
        BloomFilter<byte[]> bloomFilter1 = BloomFilter.create(Funnels.byteArrayFunnel(), size >>> 1, p);
        BloomFilter<byte[]> bloomFilter2 = BloomFilter.create(Funnels.byteArrayFunnel(), size >>> 1, p);

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();

        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
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
                long sID = spatialKeyGenerator.getNumber(cur.getLocation()) >>> 4;
                int tID = timeKeyGenerator.getNumber(cur.getDate()) >>> 2;
                for (String keyword : cur.getKeywords()) {
                    bloomFilter.put(ByteUtil.concat(Bytes.toBytes(keyword.hashCode()),
                            ByteUtil.getKByte(sID, 4),
                            ByteUtil.getKByte(tID, 3)
                    ));
                    bloomFilter1.put(ByteUtil.concat(Bytes.toBytes(keyword.hashCode()),
                            ByteUtil.getKByte(sID, 4)
                    ));
                    bloomFilter2.put(ByteUtil.concat(Bytes.toBytes(keyword.hashCode()),
                            ByteUtil.getKByte(tID, 3)
                    ));
                }

                minLat = Math.min(minLat, cur.getLat());
                minLon = Math.min(minLon, cur.getLon());
                maxLat = Math.max(maxLat, cur.getLat());
                maxLon = Math.max(maxLon, cur.getLon());
                if (cur.getDate().before(initFrom)) {
                    initFrom = cur.getDate();
                }
                if (cur.getDate().after(initEnd)) {
                    initEnd = cur.getDate();
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

        bloomFilters.add(bloomFilter);
        bloomFilters.add(bloomFilter1);
        bloomFilters.add(bloomFilter2);

        return bloomFilters;
    }

    public ArrayList<Map<BytesKey, Integer>> generateDistribution(String path) {
        Map<BytesKey, Integer> mapS = new HashMap<>();
        Map<BytesKey, Integer> mapT = new HashMap<>();
        SpatialKeyGenerator spatialKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(14);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(14);
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
                Location location = cur.getLocation();
                mapS.merge(new BytesKey(
                        ByteUtil.concat(ByteUtil.getKByte(normalizedLat.normalize(location.getLat()), 2),
                        ByteUtil.getKByte(normalizedLon.normalize(location.getLon()), 2))), 1, Integer::sum);

                mapT.merge(new BytesKey(timeKeyGenerator.toKey(cur.getDate())), 1, Integer::sum);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        ArrayList<Map<BytesKey, Integer>> result = new ArrayList<>();
        result.add(mapS);
        result.add(mapT);

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
}
