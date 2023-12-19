package org.urbcomp.startdb.stkq.preProcessing;

import com.github.nivdayan.FilterLibrary.filters.BloomFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGenerator;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BatchWrite {
    
    private static void batchInsert(String fileName, String tableName) throws IOException {
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

        if (hBaseUtil.existsTable(tableName)) {
            hBaseUtil.deleteTable(tableName);
            hBaseUtil.createTable(tableName, new String[]{"attr"});
        } else {
            hBaseUtil.createTable(tableName, new String[]{"attr"});
        }

        ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

        List<Put> puts = new ArrayList<>();
        int batchSize = 5000;
        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            long ID = 0;
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(new File(fileName).toPath())))) {
                String DELIMITER = ",";
                String line;

                while ((line = br.readLine()) != null) {

                    String[] columns = line.split(DELIMITER);

                    int n = columns.length;
                    if (n < 4) {
                        continue;
                    }

                    double lat;
                    double lon;
                    if (DataProcessor.isNumeric(columns[2]) && (DataProcessor.isNumeric(columns[3]))) {
                        lat = Double.parseDouble(columns[2]);
                        lon = Double.parseDouble(columns[3]);
                    } else
                        continue;

                    if (lat > 90 || lat < -90 || lon > 180 || lon < -180)
                        continue;

                    List<String> keywords = getStrings(columns);

                    if (keywords.isEmpty()) {
                        continue;
                    }

                    Date date = null;
                    try {
                        String time = columns[0];
                        date = sdf.parse(time);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    Location location = new Location(lat, lon);
                    byte[] sCode = sKeyGenerator.toBytes(location);
                    byte[] tCode = tKeyGenerator.toBytes(date);

                    Put put = new Put(ByteUtil.concat(sCode, tCode,  ByteUtil.longToBytesWithoutPrefixZero(ID)));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), ByteUtil.longToBytesWithoutPrefixZero(ID));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("loc"), Bytes.toBytes(location.toString()));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("time"), Bytes.toBytes(DateUtil.format(date)));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(String.join(" ", keywords)));

                    ++ID;
                    puts.add(put);

                    if (puts.size() >= batchSize) {
                        table.put(puts);
                        puts.clear();
                    }
                }
                if (!puts.isEmpty()) {
                    table.put(puts);
                    puts.clear();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            System.out.println("Dataset size: " + ID);
        }
    }

    private static void writeTweetBDIA() throws IOException {
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

        String tableName = "testTweetBDIA";

        if (hBaseUtil.existsTable(tableName)) {
            hBaseUtil.deleteTable(tableName);
            hBaseUtil.createTable(tableName, "attr", BloomType.ROWPREFIX_FIXED_LENGTH, 7, 256 * 1024 * 1024);
        } else {
            hBaseUtil.createTable(tableName, "attr", BloomType.ROWPREFIX_FIXED_LENGTH, 7, 256 * 1024 * 1024);
        }

//        String inPathName = "/home/hadoop/data/tweetAll.csv";
        String inPathName = "/usr/data/tweetAll.csv";

        ISpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            long ID = 0;
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(new File(inPathName).toPath())))) {
                String DELIMITER = ",";
                String line;

                boolean first = true;
                while ((line = br.readLine()) != null) {
                    if (first) {
                        first = false;
                        continue;
                    }

                    String[] columns = line.split(DELIMITER);

                    int n = columns.length;
                    if (n < 4) {
                        continue;
                    }

                    double lat;
                    double lon;
                    if (DataProcessor.isNumeric(columns[2]) && (DataProcessor.isNumeric(columns[3]))) {
                        lat = Double.parseDouble(columns[2]);
                        lon = Double.parseDouble(columns[3]);
                    } else
                        continue;

                    if (lat > 90 || lat < -90 || lon > 180 || lon < -180)
                        continue;

                    List<String> keywords = getStrings(columns);

                    if (keywords.isEmpty()) {
                        continue;
                    }

                    Date date = null;
                    try {
                        String time = columns[0];
                        date = sdf.parse(time);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    Location location = new Location(lat, lon);
                    byte[] sCode = sKeyGenerator.toBytes(location);
                    byte[] tCode = tKeyGenerator.toBytes(date);
                    byte[] idBytes = ByteUtil.longToBytesWithoutPrefixZero(ID);
                    byte[] tsCode = ByteUtil.concat(tCode, sCode);
                    
                    Put put = new Put(tsCode);
                    put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{1}, idBytes), Bytes.toBytes(location.toString()));
                    put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{2}, idBytes), Bytes.toBytes(DateUtil.format(date)));
                    put.addColumn(Bytes.toBytes("attr"), ByteUtil.concat(new byte[]{3}, idBytes), Bytes.toBytes(String.join(" ", keywords)));

                    byte[] bfCodes = hBaseUtil.getCell(tableName, tsCode, Bytes.toBytes("attr"), new byte[]{0});
                    BloomFilter bf;
                    if (bfCodes == null || bfCodes.length == 0) {
                        bf = new BloomFilter(100, 10);
                    } else {
                        bf = new BloomFilter(bfCodes, 7, 1000);
                    }
                    for (String s : keywords) {
                        bf.insert(s, false);
                    }
                    bfCodes = bf.getArray();
                    put.addColumn(Bytes.toBytes("attr"), new byte[]{0}, bfCodes);

                    ++ID;
                    table.put(put);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            System.out.println("Dataset size: " + ID);
        }
    }

    private static List<String> getStrings(String[] columns) {
        List<String> keywords = new ArrayList<>();

        String keywordStr = columns[1];
        int len = keywordStr.length();
        StringBuilder builder = new StringBuilder();
        for (int j = 0; j < len; ++j) {
            if (DataProcessor.isAlphabet(keywordStr.charAt(j))) {
                builder.append(keywordStr.charAt(j));
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

    public static void main(String[] args) throws IOException, ParseException {
        batchInsert("/usr/data/yelp.csv", "testYelp");
        writeTweetBDIA();
    }
}
