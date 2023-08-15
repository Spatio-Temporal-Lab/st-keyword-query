package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.keyGenerator.old.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.TimeKeyGenerator;
import com.github.davidmoten.hilbert.hilbert.HilbertCurve;
import com.github.davidmoten.hilbert.hilbert.SmallHilbertCurve;
import org.locationtech.geomesa.curve.NormalizedDimension;
import org.urbcomp.startdb.stkq.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class QueryGenerator {

    private static ArrayList<String> keywords;
    private static final Random random = new Random();

    private static ArrayList<String> getRandomKeywords() {
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
                writer.write(mbr.getMinLat() + "," + mbr.getMaxLat());
                writer.write(",");
                writer.write(mbr.getMinLon() + "," + mbr.getMaxLon());
                writer.write(",");
                Date date = object.getTime();
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, -120)));
                writer.write(",");
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, 120)));
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
        List<BytesKey> stKeySortByObjectCount;
        Map<BytesKey, Set<String>> key2Words;

        try(FileInputStream fin = new FileInputStream("/usr/data/st2Count.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            stKeySortByObjectCount = MapUtil.sortByValue((Map<BytesKey, Integer>) ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try(FileInputStream fin = new FileInputStream("/usr/data/st2Words.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            key2Words = (Map<BytesKey, Set<String>>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        ZipfGenerator stKeyGenerator = new ZipfGenerator(stKeySortByObjectCount.size(), skew);

        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(14);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(14);
        SmallHilbertCurve curve = HilbertCurve.small().bits(14).dimensions(2);

        String path = new File("").getAbsolutePath() + "/st-keyword-query/src/main/resources/queriesZipf.csv";
        System.out.println(path);
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {

            int writeCount = 0;
            while (writeCount < size) {

                int stId = stKeySortByObjectCount.size() - stKeyGenerator.next();
                BytesKey bytesKey = stKeySortByObjectCount.get(stId);
                byte[] bytesKeyArray = bytesKey.getArray();
                byte[] sKey = Arrays.copyOfRange(bytesKeyArray, 0, 4);

                byte[] tKey = Arrays.copyOfRange(bytesKeyArray, 4, 7);

                long sKeyLong = ByteUtil.toLong(sKey);
                long[] originS = curve.point(sKeyLong);
                double lat = normalizedLat.denormalize((int) originS[0]);
                double lon = normalizedLon.denormalize((int) originS[1]);

                MBR mbr = GeoUtil.getMBRByCircle(new Location(lat, lon), 4000);
                writer.write(mbr.getMinLat() + "," + mbr.getMaxLat());
                writer.write(",");
                writer.write(mbr.getMinLon() + "," + mbr.getMaxLon());
                writer.write(",");

                Date date = DateUtil.getDateAfterHours(ByteUtil.toInt(tKey));
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, -120)));
                writer.write(",");
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, 120)));
                ArrayList<String> keywords;

                if (random.nextDouble() < 0.5) {
                    keywords = getRandomKeywords(new ArrayList<>(key2Words.get(bytesKey)));
                } else {
                    keywords = getRandomKeywords();
                }

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

    public static void generateZipfQueries(ArrayList<STObject> objects, int size, double skew) {
        List<BytesKey> stKeySortByObjectCount;
        Map<BytesKey, Set<String>> key2Words = new HashMap<>();
        Map<BytesKey, Integer> key2Count = new HashMap<>();


        SpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();
        for (STObject object : objects) {
            BytesKey key = new BytesKey(ByteUtil.concat(
                    sKeyGenerator.toKey(object.getLocation()), tKeyGenerator.toKey(object.getTime())));
            key2Count.merge(key, 1, Integer::sum);

            Set<String> set = key2Words.get(key);
            if (set == null) {
                set = new HashSet<>(object.getKeywords());
                key2Words.put(key, set);
            } else {
                set.addAll(object.getKeywords());
            }
        }
        stKeySortByObjectCount = MapUtil.sortByValue(key2Count);

        ZipfGenerator stKeyGenerator = new ZipfGenerator(stKeySortByObjectCount.size(), skew);

        NormalizedDimension.NormalizedLat normalizedLat = new NormalizedDimension.NormalizedLat(14);
        NormalizedDimension.NormalizedLon normalizedLon = new NormalizedDimension.NormalizedLon(14);
        SmallHilbertCurve curve = HilbertCurve.small().bits(14).dimensions(2);

        String path = new File("").getAbsolutePath() + "/src/main/resources/queriesZipfSample.csv";
        System.out.println(path);
        File file = new File(path);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {

            int writeCount = 0;
            while (writeCount < size) {

                int stId = stKeySortByObjectCount.size() - stKeyGenerator.next();
                BytesKey bytesKey = stKeySortByObjectCount.get(stId);
                byte[] bytesKeyArray = bytesKey.getArray();
                byte[] sKey = Arrays.copyOfRange(bytesKeyArray, 0, 4);

                byte[] tKey = Arrays.copyOfRange(bytesKeyArray, 4, 7);

                long sKeyLong = ByteUtil.toLong(sKey);
                long[] originS = curve.point(sKeyLong);
                double lat = normalizedLat.denormalize((int) originS[0]);
                double lon = normalizedLon.denormalize((int) originS[1]);

                MBR mbr = GeoUtil.getMBRByCircle(new Location(lat, lon), 4000);
                writer.write(mbr.getMinLat() + "," + mbr.getMaxLat());
                writer.write(",");
                writer.write(mbr.getMinLon() + "," + mbr.getMaxLon());
                writer.write(",");

                Date date = DateUtil.getDateAfterHours(ByteUtil.toInt(tKey));
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, -120)));
                writer.write(",");
                writer.write(DateUtil.format(DateUtil.getDateAfterMinutes(date, 120)));
                ArrayList<String> keywords;

                if (random.nextDouble() < 0.5) {
                    keywords = getRandomKeywords(new ArrayList<>(key2Words.get(bytesKey)));
                } else {
                    keywords = getRandomKeywords();
                }

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
        try(FileInputStream fin = new FileInputStream("src/main/resources/keywords.txt");
            ObjectInputStream ois = new ObjectInputStream(fin)
        ) {
            keywords = new ArrayList<>((Set<String>) ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        ArrayList<STObject> objects;
        String path = "src/main/resources/tweetSample.txt";
        try(InputStream is = Files.newInputStream(Paths.get(path));
            ObjectInputStream ois = new ObjectInputStream(is)) {
            objects = (ArrayList<STObject>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        generateZipfQueries(objects, 10000, 0.8);
    }
}
