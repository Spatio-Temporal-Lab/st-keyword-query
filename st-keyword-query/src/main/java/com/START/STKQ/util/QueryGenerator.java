package com.START.STKQ.util;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.model.Location;
import com.START.STKQ.model.MBR;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.STObject;

import java.io.*;
import java.text.ParseException;
import java.util.*;

public class QueryGenerator {
    private static final Random random = new Random();

    private static ArrayList<String> getRandomKeywords() {
        ArrayList<String> keywords = KeywordCounter.getKeywords();
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

    public static void generateQueries(ArrayList<STObject> objects) throws IOException {
//        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queries.csv";
        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queriesForSample.csv";
        System.out.println(path);
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            int n = objects.size();
            int writeCount = 0;
            while (writeCount < 2000) {
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
                if (writeCount <= 1000) {
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

    public static void main(String[] args) throws IOException, ParseException {
        DataReader dataReader = new DataReader();
//        dataReader.setRate(0.001);
        ArrayList<STObject> objects = new ArrayList<>(dataReader.getSTObjects("/usr/data/tweetSample.csv"));
        generateQueries(objects);
    }
}
