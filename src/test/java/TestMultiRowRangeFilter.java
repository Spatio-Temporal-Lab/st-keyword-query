import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.urbcomp.startdb.stkq.io.HBaseUtil;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestMultiRowRangeFilter {
    static String tableName = "test";
    static int count = 1000;
    static int gap = 100;
    static HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
    @Test
    public void testMultiRowRangeFilter() {
        long begin = System.currentTimeMillis();
        List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            rowRanges.add(new MultiRowRangeFilter.RowRange(
                    ByteUtil.getKByte(i * gap, 4), true,
                    ByteUtil.getKByte(i * gap + gap - 2, 4), true));
        }
        MultiRowRangeFilter filter = new MultiRowRangeFilter(rowRanges);
        List<Map<String, String>> result = hBaseUtil.scan(tableName, rowRanges.get(0).getStartRow(), rowRanges.get(rowRanges.size() - 1).getStopRow(), filter);
        System.out.println(System.currentTimeMillis() - begin);
    }

    @Test
    public void testWithoutFilter() {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            List<Map<String, String>> result = hBaseUtil.scan(tableName, ByteUtil.getKByte(i * gap, 4),
                    ByteUtil.getKByte(i * gap + gap - 2, 4));
        }
        System.out.println(System.currentTimeMillis() - begin);
    }

    @BeforeClass
    public static void initFilterTable() throws IOException {
        if (!hBaseUtil.existsTable(tableName)) {
            hBaseUtil.createTable(tableName, "attr", BloomType.ROW);
        } else {
            hBaseUtil.truncateTable(tableName);
        }

        try (Table table = hBaseUtil.getConnection().getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            for (int i = 0; i < count * gap; ++i) {
                byte[] rowkey = ByteUtil.getKByte(i, 4);
                Put put = new Put(rowkey);
                put.addColumn("attr".getBytes(), "value".getBytes(), rowkey);
                puts.add(put);
            }
            table.put(puts);
        }
    }
}
