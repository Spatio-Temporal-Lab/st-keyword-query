package org.urbcomp.startdb.stkq.preProcessing;

import org.urbcomp.startdb.stkq.filter.ISTKFilter;
import org.urbcomp.startdb.stkq.filter.STKFilter;
import org.urbcomp.startdb.stkq.filter.manager.BasicFilterManager;
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
    private static long ID;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSampleBig.csv";

    static String DELIMITER = ",";

    public DataProcessor() {
        limit = 10000_0000;
        rate = 1.0;
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
        if (!str.isEmpty()) {
            if (str.startsWith("-")) {
                tmp = str.substring(1);
            } else {
                tmp = str;
            }
            flag = tmp.matches("^[\\d.]+$");
        }
        return flag;
    }

    public static boolean isAlphabet(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    public static STObject parseSTObject(String line) {
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

        List<String> keywords = getKeywords(columns);

        if (keywords.isEmpty()) {
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

    private static List<String> getKeywords(String[] columns) {
        List<String> keywords = new ArrayList<>();

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
        return keywords;
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

        List<String> keywords = getKeywords(columns);

        if (keywords.isEmpty()) {
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

    public List<STObject> getSTObjects(String path) throws ParseException {

        double maxLat = -100.0;
        double minLat = 100.0;
        double maxLon = -200.0;
        double minLon = 200.0;

        Random random = new Random();

        //实现对象读取
        String dateString = "1900-02-23 00:00";
        Date initEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dateString);
        Date initFrom = new Date();
        List<STObject> shops = new ArrayList<>(limit);

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            while ((line = br.readLine()) != null) {

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

    public void putFiltersToRedis(ISTKFilter stFilter, String path) throws ParseException {
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

            while ((line = br.readLine()) != null) {

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

        System.out.println(stFilter.ramUsage());
        stFilter.out();
        System.out.println(minLat);
        System.out.println(minLon);
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(initFrom);
        System.out.println(initEnd);
    }

    public List<Map> generateDistribution(String path) {
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

        List<Map> result = new ArrayList<>();
        result.add(st2Count);
        result.add(st2Keywords);
        return result;
    }

    public Set<String> generateKeywords(String path) {
        Set<String> set = new TreeSet<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(path).toPath())))) {
            String line;

            while ((line = br.readLine()) != null) {
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
        int sBits = 8;
        int tBits = 4;
        BasicFilterManager manager = new BasicFilterManager(3, 18);
        ISTKFilter stFilter = new STKFilter(sBits, tBits, manager);
        dataProcessor.putFiltersToRedis(stFilter, "/usr/data/yelp.csv");
    }
}
