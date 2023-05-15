package com.START.STKQ.io;

import com.START.STKQ.constant.QueryType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HBaseUtil {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    // 建立连接
    public void init(String zookeeper) {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", zookeeper);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HBaseUtil getDefaultHBaseUtil() {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.init("master,slave1,slave2");
        return hBaseUtil;
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
            admin.createTable(builder.build());
        }
        return true;
    }

    public boolean createTable(String myTableName, String colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
            return false;
        } else {
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(colFamily));
            admin.createTable(builder.build());
        }
        return true;
    }

    public Table getTable(String tableName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            return table;
        }
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

    // 插入多行多列数据
    public void put(String tableName,
                    List<byte[]> rowKey, String columnFamily, List<String> columns, List<String> data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            int i = 0;
            for (byte[] bytes : rowKey) {
                Put put = new Put(bytes);
                for (String column : columns) {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data.get(i)));
                    ++i;
                }
                puts.add(put);
            }
            table.put(puts);
        }
    }

    // 插入多行一列数据
    public void put(String tableName,
                    List<byte[]> rowKey, String columnFamily, String column, List<String> data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            int n = rowKey.size();
            for (int i = 0; i < n; ++i) {
                Put put = new Put(rowKey.get(i));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data.get(i)));
                puts.add(put);
            }
            table.put(puts);
        }
    }

    // 插入多行一列数据（数据全部相同）
    public void put(String tableName,
                    List<byte[]> rowKey, String columnFamily, String column, String data) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            for (byte[] bytes : rowKey) {
                Put put = new Put(bytes);
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
                puts.add(put);
            }
            table.put(puts);
        }
    }

    // 扫描一格内容
    public String getCell(String tableName, byte[] rowKey, String columnFamily, String column) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowKey);
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return null;
            }
            return new String(CellUtil.cloneValue(cells.get(0)), StandardCharsets.UTF_8);
        }
    }

    // 扫描多格内容
    public List<String> getCells(String tableName, List<String> rowKeys, String columnFamily, String column) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Get> gets = new ArrayList<>();
            for (String rowKey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowKey));
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                gets.add(get);
            }
            Result[] results = table.get(gets);
            List<String> answers = new ArrayList<>();
            for (Result result : results) {
                List<Cell> cells = result.listCells();
                answers.add(new String(CellUtil.cloneValue(cells.get(0)), StandardCharsets.UTF_8));
            }
            return answers;
        }
    }

    // 扫描一行内容
    public Map<String, String> getRow(String tableName, String rowKey) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));

            Result result = table.get(get);
            List<Cell> cells = result.listCells();

            if (CollectionUtils.isEmpty(cells)) {
                return Collections.emptyMap();
            }
            Map<String, String> objectMap = new HashMap<>();
            for (Cell cell : cells) {
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                objectMap.put(qualifier, value);
            }
            return objectMap;
        }
    }

    // 扫描多行内容
    public List<Map<String, String>> scan(String tableName, byte[] rowkeyStart, byte[] rowkeyEnd) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                scan.withStartRow(rowkeyStart);
                scan.withStopRow(rowkeyEnd);

                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = new ArrayList<>();
                for (Result r : rs) {
                    Map<String, String> objectMap = new HashMap<>();
                    objectMap.put("rowkey", Arrays.toString(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
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
        }
    }

    // 扫描所有内容
    public List<Map<String, String>> scanAll(String tableName) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();

                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = new ArrayList<>();
                for (Result r : rs) {
                    Map<String, String> objectMap = new HashMap<>();
                    objectMap.put("rowkey", Arrays.toString(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
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


    public List<Map<String, String>> scanWithKeywords(String tableName, ArrayList<String> keywords,
                                                      byte[] rowkeyStart, byte[] rowkeyEnd, QueryType queryType) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            ResultScanner rs = null;
            try {
                Scan scan = new Scan();
                scan.withStartRow(rowkeyStart);
                scan.withStopRow(rowkeyEnd, true);

                rs = table.getScanner(scan);

                List<Map<String, String>> dataList = new ArrayList<>();

                for (Result r : rs) {
                    Map<String, String> objectMap = new HashMap<>();
                    objectMap.put("rowkey", Arrays.toString(r.getRow()));
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value = new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8);
                        objectMap.put(qualifier, value);
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
