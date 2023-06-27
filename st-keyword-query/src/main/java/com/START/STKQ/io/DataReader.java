package com.START.STKQ.io;

import com.START.STKQ.keyGenerator.*;
import com.START.STKQ.model.*;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import com.START.STKQ.util.KeywordCounter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.hbase.util.Bytes;

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

//            int n = 0;
            while ((line = br.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;
                }

                STObject cur = getSTObject(line);
//                System.out.println(cur);
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

//                if (++n > 5) {
//                    break;
//                }
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
}
