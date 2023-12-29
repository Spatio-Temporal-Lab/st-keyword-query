package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.BloomFilter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HBaseUtil {
    public static Configuration configuration;
    public static Connection connection;

    public static Admin admin;
    private static final HBaseUtil defaultUtil;

    static {
        defaultUtil = new HBaseUtil();
        defaultUtil.init("10.242.6.16:2181,10.242.6.17:2181,10.242.6.18:2181,10.242.6.19:2181,10.242.6.20:2181");
    }

    // 建立连接
    public void init(String zookeeper) {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeper);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseUtil getDefaultHBaseUtil() {
        return defaultUtil;
    }

    public Connection getConnection() {
        return connection;
    }

    // 关闭连接
    public void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 建表
    public boolean createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
            return false;
        } else {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            for (String columnFamily : colFamily) {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
            }
            builder.setMaxFileSize(256 * 1024 * 1024);
            admin.createTable(builder.build());
        }
        return true;
    }

    public boolean createTable(String myTableName, String colFamily, BloomType bloomType, int preLen, long maxFileSize) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
            return false;
        } else {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(colFamily.getBytes());
            columnFamilyDescriptorBuilder.setBloomFilterType(bloomType);
            columnFamilyDescriptorBuilder.setConfiguration("RowPrefixBloomFilter.prefix_length", String.valueOf(preLen));
            builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            builder.setMaxFileSize(maxFileSize);
            admin.createTable(builder.build());
        }
        return true;
    }

    public boolean createTable(String myTableName, String colFamily, BloomType bloomType) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
            return false;
        } else {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(colFamily.getBytes());
            columnFamilyDescriptorBuilder.setBloomFilterType(bloomType);
            if (bloomType.equals(BloomType.ROWPREFIX_FIXED_LENGTH)) {
                columnFamilyDescriptorBuilder.setConfiguration("RowPrefixBloomFilter.prefix_length", String.valueOf(6));
            }
            builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.createTable(builder.build());
        }
        return true;
    }

    public Table getTable(String tableName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table;
        }
    }

    public void flushTable(String tableName) throws IOException {
        admin.flush(TableName.valueOf(tableName));
    }

    public void createTableAndDeleteOld(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        for (String columnFamily : colFamily) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
        }
        admin.createTable(builder.build());
    }

    // 插入单行数据
    public void put(String tableName,
                    byte[] rowKey, String columnFamily, String column, String data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        }
    }

    public void put(String tableName,
                    byte[] rowKey, String columnFamily, String column, byte[] data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), data);
            table.put(put);
        }
    }

    public void putIfNotExist(String tableName,
                    byte[] rowKey, String columnFamily, String column, byte[] data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey);
            byte[] family = Bytes.toBytes(columnFamily);
            byte[] qualifier = Bytes.toBytes(column);
            put.addColumn(family, qualifier, data);
            table.checkAndPut(rowKey, family, qualifier, null, put);
        }
    }

    // 扫描一格内容
    public byte[] getCell(String tableName, byte[] rowKey, String columnFamily, String column) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowKey);
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return null;
            }
            return CellUtil.cloneValue(cells.get(0));
        }
    }

    public byte[] getCell(String tableName, byte[] rowKey, byte[] columnFamily, byte[] column) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowKey);
            get.addColumn(columnFamily, column);

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return null;
            }
            return CellUtil.cloneValue(cells.get(0));
        }
    }

    public List<Map<String, String>> scan(String tableName, byte[] rowkeyStart, byte[] rowkeyEnd, MultiRowRangeFilter filter) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                scan.setFilter(filter);

                scan = scan.withStartRow(rowkeyStart, true);
                scan = scan.withStopRow(rowkeyEnd, true);
                scan.setCaching(1000);
                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = new ArrayList<>();
                for (Result r : rs) {
                    Map<String, String> objectMap = new HashMap<>();
                    objectMap.put("rowkey", Arrays.toString(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value;
                        if (qualifier.equals("id")) {
                            value = String.valueOf(Bytes.toLong(CellUtil.cloneValue(cell)));
                        }
                        else {
                            value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                        }
                        objectMap.put(qualifier, value);
                    }
                    dataList.add(objectMap);
                }
                return dataList;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, String>> scanSTK(String tableName, byte[] rowkeyStart, byte[] rowkeyEnd) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                scan.withStartRow(rowkeyStart);
                scan.withStopRow(rowkeyEnd, true);

                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = Collections.synchronizedList(new ArrayList<>());
                for (Result r : rs) {
                    Map<String, String> objectMap = new Hashtable<>();
                    objectMap.put("rowkey", new String(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value;
                        if (qualifier.equals("id")) {
                            value = String.valueOf(ByteUtil.toLong(CellUtil.cloneValue(cell)));
                        }
                        else {
                            value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                        }
                        objectMap.put(qualifier, value);
                    }
                    dataList.add(objectMap);
                }
                return dataList;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Map<String, String>> scanBDIA(String tableName, String[] keywords, byte[] rowkeyStart, byte[] rowkeyEnd, QueryType queryType) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                scan.withStartRow(rowkeyStart);
                scan.withStopRow(rowkeyEnd, true);

                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = Collections.synchronizedList(new ArrayList<>());
                for (Result r : rs) {
                    Map<BytesKey, String> locMap = new HashMap<>();
                    Map<BytesKey, String> timeMap = new HashMap<>();
                    Map<BytesKey, String> keywordsMap = new HashMap<>();

                    for (Cell cell : r.listCells()) {
                        byte[] qualifier = CellUtil.cloneQualifier(cell);
                        if (Arrays.equals(qualifier, new byte[]{0})) {
                            BloomFilter bf = new BloomFilter(CellUtil.cloneValue(cell), 7, 1000);
                            if (!STKUtil.check(bf, keywords, queryType)) {
                                break;
                            }
                        } else {
                            byte[] idBytes = Arrays.copyOfRange(qualifier, 1, qualifier.length);
                            String value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                            switch (qualifier[0]) {
                                case 1: {
                                    locMap.put(new BytesKey(idBytes), value);
                                    break;
                                }
                                case 2: {
                                    timeMap.put(new BytesKey(idBytes), value);
                                    break;
                                }
                                case 3: {
                                    keywordsMap.put(new BytesKey(idBytes), value);
                                    break;
                                }
                                default:
                                    System.err.println("error");
                            }
                        }
                    }

                    for (Map.Entry<BytesKey, String> entry : locMap.entrySet()) {
                        BytesKey id = entry.getKey();
                        byte[] idBytes = id.getArray();
                        Map<String, String> mss = new HashMap<>();
                        // value = String.valueOf(ByteUtil.toLong(CellUtil.cloneValue(cell)));
                        mss.put("id", String.valueOf(ByteUtil.toLong(idBytes)));
                        mss.put("loc", locMap.get(id));
                        mss.put("time", timeMap.get(id));
                        mss.put("keywords", keywordsMap.get(id));
                        dataList.add(mss);
                    }
                }

                return dataList;
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 删除表
    public void deleteTable(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                //先执行disable
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
        }
    }

    public boolean existsTable(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        }
    }

    public void truncateTable(String tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                //先执行disable
                admin.disableTable(TableName.valueOf(tableName));
                admin.truncateTable(TableName.valueOf(tableName), true);
            }
        }
    }

    public int getBlockSize(String tableName, String columnFamilyName) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ColumnFamilyDescriptor familyDescriptor = table.getDescriptor().getColumnFamily(columnFamilyName.getBytes());
            return familyDescriptor.getBlocksize();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public ArrayList<String> getTables() throws IOException {
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        ArrayList<String> names = new ArrayList<>();
        for (TableDescriptor descriptor : tableDescriptors) {
            names.add(descriptor.getTableName().getNameAsString());
        }
        return names;
    }
}
