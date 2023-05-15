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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TestWriteTweetAll {

    public static void main(String[] args) throws IOException, ParseException {

        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();

        String tableName = "testTweet";
//        hBaseUtil.createTable(tableName, new String[]{"attr"});
        hBaseUtil.truncateTable(tableName);

        String inPathName = "/home/liruiyuan/Tweetsall.csv";
//        String inPathName = "E:\\data\\tweet\\tweet_2.csv";

        SpatialKeyGenerator sKeyGenerator = new HilbertSpatialKeyGenerator();
        TimeKeyGenerator tKeyGenerator = new TimeKeyGenerator();

        List<Put> puts = new ArrayList<>();

        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            String failTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("fail time :" + failTime + " ,insert data fail,cause：" + e.getCause(0) + "，failed num：" + e.getNumExceptions());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            //重试
            String retryTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
            System.out.println("its time to retry:" + retryTime);
            for (int i = 0; i < e.getNumExceptions(); i++) {
                org.apache.hadoop.hbase.client.Row row = null;
                try {
                    row = e.getRow(i);
                    mutator.mutate((Put) row);
                } catch (IOException ex) {
                    System.out.println("insert data fail,please check hbase status and row info : " + row);
                }
            }
        };

        BufferedMutatorParams htConfig = new BufferedMutatorParams(TableName.valueOf(tableName)).writeBufferSize(10 * 1024 * 1024).listener(listener);

        int batchSize = 5000;
        try (BufferedMutator table = hBaseUtil.getConnection().getBufferedMutator(htConfig)) {

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

                    Put put = new Put(ByteUtil.concat(sCode, tCode,  Bytes.toBytes(ID)));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("id"), Bytes.toBytes(ID++));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("loc"), Bytes.toBytes(location.toString()));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("time"), Bytes.toBytes(DateUtil.format(date)));
                    put.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("keywords"), Bytes.toBytes(String.join(" ", keywords)));
                    puts.add(put);

                    if (puts.size() >= batchSize) {
                        table.mutate(puts);
                        puts.clear();
                    }
                }
                if (!puts.isEmpty()) {
                    table.mutate(puts);
                    puts.clear();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            System.out.println("Dataset size: " + ID);
        }


        long rowCount = 0;
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner resultScanner = hBaseUtil.getTable(tableName).getScanner(scan);
        for (Result result : resultScanner) {
            rowCount += result.size();
        }
        String outPathName = "/home/liruiyuan/logBloomSize.txt";
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outPathName), StandardCharsets.UTF_8)) {
            writer.write(String.valueOf(rowCount));
        }
        System.out.println(rowCount);
    }

}
