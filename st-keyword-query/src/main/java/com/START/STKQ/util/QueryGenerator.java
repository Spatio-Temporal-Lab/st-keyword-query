package com.START.STKQ.util;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.model.*;
import org.locationtech.geomesa.curve.NormalizedDimension;

import java.io.*;
import java.text.ParseException;
import java.util.*;

public class QueryGenerator {

    private static ArrayList<String> keywords;
    private static final Random random = new Random();

    private static ArrayList<String> getRandomKeywords() {
//        ArrayList<String> keywords = KeywordCounter.getKeywords();
        int n = keywords.size();
        int m = random.nextInt(Math.min(n, 3)) + 1;
        Set<String> set = new HashSet<>();
        while (set.size() < m) {
            set.add(keywords.get(random.nextInt(n)));
        }
        return new ArrayList<>(set);
    }

    private static ArrayList<String> getRandomKeywords(ArrayList<String> keywords) {
        ArrayList<String> keywords1 = new ArrayList<>(keywords);
        Collections.shuffle(keywords1);
        int n = keywords1.size();
        int m = random.nextInt(Math.min(n, 3)) + 1;
        if (m == n) {
            return keywords1;
        }
        System.out.println(m + " " + n);
        return new ArrayList<>(keywords1.subList(0, m));
    }

    public static ArrayList<Query> getQueries() {
        return getQueries("queries.csv");
    }

    public static ArrayList<Query> getQueries(String fileName) {
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

    public static void generateQueries(ArrayList<STObject> objects, int count) throws IOException {
        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queries.csv";
//        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queriesForSample.csv";
        System.out.println(path);
        int half = count / 2;
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            int n = objects.size();
            int writeCount = 0;
            while (writeCount < count) {
                STObject object = objects.get(random.nextInt(n));
                if (object.getLat() > 179 || object.getLat() < -179) {
                    continue;
                }
                if (object.getLon() > 89 || object.getLon() < -89) {
                    continue;
                }
                if (object.getKeywords().size() == 0) {
                    continue;
                }
                double lat = object.getLat();
                double lon = object.getLon();
                MBR mbr = GeoUtil.getMBRByCircle(new Location(lat, lon), 4000);
                writer.write(mbr.getMinLatitude() + "," + mbr.getMaxLatitude());
                writer.write(",");
                writer.write(mbr.getMinLongitude() + "," + mbr.getMaxLongitude());
                writer.write(",");
                Date date = object.getDate();
                writer.write(DateUtil.format(DateUtil.getDateAfter(date, -120)));
                writer.write(",");
                writer.write(DateUtil.format(DateUtil.getDateAfter(date, 120)));
                ArrayList<String> keywords;
                if (writeCount < half) {
                    keywords = getRandomKeywords();
                } else {
                    keywords = getRandomKeywords(object.getKeywords());
                }
                for (String keyword : keywords) {
                    writer.write("," + keyword);
                }
                writer.newLine();
                ++writeCount;
                System.out.println(writeCount);
            }
        }
    }

    public static void generateZipfQueries(int size, double skew) {

        ArrayList<BytesKey> sKeySortByObjectCount;
        ArrayList<BytesKey> tKeySortByObjectCount;

        try(FileInputStream fin = new FileInputStream("/usr/data/count0.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            Map<BytesKey, Integer> spatialDistribution = (Map<BytesKey, Integer>) ois.readObject();
            sKeySortByObjectCount = MapUtil.sortByValue(spatialDistribution);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try(FileInputStream fin = new FileInputStream("/usr/data/count1.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            Map<BytesKey, Integer> timeDistribution = (Map<BytesKey, Integer>) ois.readObject();
            tKeySortByObjectCount = MapUtil.sortByValue(timeDistribution);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        ZipfGenerator sKeyGenerator = new ZipfGenerator(sKeySortByObjectCount.size(), skew);
        ZipfGenerator tKeyGenerator = new ZipfGenerator(tKeySortByObjectCount.size(), skew);
        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(14);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(14);

        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queriesZipf.csv";
        System.out.println(path);
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {

            int writeCount = 0;
            while (writeCount < size) {

                int sKeyId = sKeySortByObjectCount.size() - sKeyGenerator.next();
                int tKeyId = tKeySortByObjectCount.size() - tKeyGenerator.next();

                byte[] sBytesKey = sKeySortByObjectCount.get(sKeyId).getArray();
                byte[] latKey = Arrays.copyOfRange(sBytesKey, 0, 2);
                byte[] lonKey = Arrays.copyOfRange(sBytesKey, 2, 4);
                System.out.println(Arrays.toString(sBytesKey));
                System.out.println(Arrays.toString(latKey) + " " + Arrays.toString(lonKey));
                double lat = normalizedLat.denormalize(ByteUtil.toInt(latKey));
                double lon = normalizedLon.denormalize(ByteUtil.toInt(lonKey));
                System.out.println(ByteUtil.toInt(latKey));
                System.out.println(lat);
                System.out.println(ByteUtil.toInt(lonKey));
                System.out.println(lon);
                MBR mbr = GeoUtil.getMBRByCircle(new Location(lat, lon), 4000);
                writer.write(mbr.getMinLatitude() + "," + mbr.getMaxLatitude());
                writer.write(",");
                writer.write(mbr.getMinLongitude() + "," + mbr.getMaxLongitude());
                writer.write(",");

                Date date = DateUtil.getDateAfter(ByteUtil.toInt(tKeySortByObjectCount.get(tKeyId).getArray()));
                writer.write(DateUtil.format(DateUtil.getDateAfter(date, -120)));
                writer.write(",");
                writer.write(DateUtil.format(DateUtil.getDateAfter(date, 120)));
                ArrayList<String> keywords;

                keywords = getRandomKeywords();

                for (String keyword : keywords) {
                    writer.write("," + keyword);
                }
                writer.newLine();
                ++writeCount;
                System.out.println(writeCount);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        try(FileInputStream fin = new FileInputStream("/usr/data/keywords.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            keywords = new ArrayList<>((Set<String>) ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

//        DataReader dataReader = new DataReader();

//        dataReader.setRate(0.1);
//        ArrayList<STObject> objects = new ArrayList<>(dataReader.getSTObjects("/usr/data/tweetSample.csv"));
//        ArrayList<STObject> objects = new ArrayList<>(dataReader.getSTObjects("/usr/data/tweetAll.csv"));
//        generateQueries(objects, 1_0000);
//
        generateZipfQueries(10000, 0.8);
    }
}
