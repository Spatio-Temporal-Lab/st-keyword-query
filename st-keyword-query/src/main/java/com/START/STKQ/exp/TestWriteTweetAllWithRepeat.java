package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.io.HBaseUtil;
import com.START.STKQ.keyGenerator.HilbertSpatialKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialKeyGenerator;
import com.START.STKQ.keyGenerator.TimeKeyGenerator;
import com.START.STKQ.model.Location;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

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
import java.util.Random;

public class TestWriteTweetAllWithRepeat {

    public static void main(String[] args) throws IOException, ParseException {

        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("192.168.137.204");

        String tableName = "testTweetSample";
        hBaseUtil.deleteTable(tableName);
        hBaseUtil.createTable(tableName, "attr", BloomType.ROWPREFIX_WITH_KEYWORDS, 7, 256 * 1012 * 1024);

        String inPathName = "/usr/data/tweetSample.csv";

        SpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

        List<Put> puts = new ArrayList<>();

        Random random = new Random();

        int batchSize = 5000;
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
                    if (DataReader.isNumeric(columns[2]) && (DataReader.isNumeric(columns[3]))) {
                        lat = Double.parseDouble(columns[2]);
                        lon = Double.parseDouble(columns[3]);
                    } else
                        continue;

                    if (lat > 90 || lat < -90 || lon > 180 || lon < -180)
                        continue;

                    ArrayList<String> keywords = new ArrayList<>();

                    String keywordStr = columns[1];
                    int len = keywordStr.length();
                    StringBuilder builder = new StringBuilder();
                    for (int j = 0; j < len; ++j) {
                        if (DataReader.isAlphabet(keywordStr.charAt(j))) {
                            builder.append(keywordStr.charAt(j));
                        } else if (builder.length() != 0) {
                            keywords.add(builder.toString());
                            builder = new StringBuilder();
                        }
                    }
                    if (builder.length() > 0) {
                        keywords.add(builder.toString());
                    }

                    if (keywords.size() == 0) {
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
                    byte[] sCode = sKeyGenerator.toKey(location);
                    byte[] tCode = tKeyGenerator.toKey(date);

                    for (int i = 0; i < 1000; ++i) {
                        Put put = new Put(ByteUtil.concat(sCode, tCode, Bytes.toBytes(ID)));
                        put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), Bytes.toBytes(ID++));
                        put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("loc"), Bytes.toBytes(location.toString()));
                        put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("time"), Bytes.toBytes(DateUtil.format(date)));
                        put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(String.join(" ", keywords)));
                        puts.add(put);
                    }

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


//        long rowCount = 0;
//        Scan scan = new Scan();
//        scan.setFilter(new FirstKeyOnlyFilter());
//        ResultScanner resultScanner = hBaseUtil.getTable(tableName).getScanner(scan);
//        for (Result result : resultScanner) {
//            rowCount += result.size();
//        }
//        String outPathName = "/usr/data//log/rowCount.txt";
//        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
//            writer.write(String.valueOf(rowCount));
//        }
//        System.out.println(rowCount);
    }

}
