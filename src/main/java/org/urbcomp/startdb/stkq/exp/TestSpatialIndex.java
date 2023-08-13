package org.urbcomp.startdb.stkq.exp;

import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.model.MBR;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.GeoUtil;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class TestSpatialIndex {

    private static long getSize(ArrayList<Range<byte[]>> ranges) {
        long size = 0;
        for (Range<byte[]> range : ranges) {
            size += ByteUtil.toLong(range.getHigh()) - ByteUtil.toLong(range.getLow());
        }
        return size;
    }

    private static boolean check(byte[] key, ArrayList<Range<byte[]>> keyRanges) {
        long keyLong = ByteUtil.toLong(key);
        for (Range<byte[]> range : keyRanges) {
            long low = ByteUtil.toLong(range.getLow());
            long high = ByteUtil.toLong(range.getHigh());
            if (keyLong >= low && keyLong <= high) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws ParseException {
        SpatialKeyGenerator keyGenerator1 = new SpatialKeyGenerator();
        HilbertSpatialKeyGenerator keyGenerator2 = new HilbertSpatialKeyGenerator();

        DataProcessor dataProcessor = new DataProcessor();
        dataProcessor.setLimit(10_00000);
        ArrayList<STObject> stObjects = dataProcessor.getSTObjects("E:\\data\\tweet\\tweet_2.csv");

        long size1Sum = 0;
        long size2Sum = 0;

        ArrayList<Query> queries = new ArrayList<>();
        for (STObject object : stObjects) {
            MBR mbr = GeoUtil.getMBRByCircle(object.getLocation(), 1000);
            queries.add(new Query(mbr, new Date(), new Date(), new ArrayList<>()));
        }

        int n = queries.size();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            byte[] key1 = keyGenerator1.toKey(stObjects.get(i).getLocation());
            ArrayList<Range<byte[]>> ranges1 = keyGenerator1.toKeyRanges(queries.get(i));
            if (!check(key1, ranges1)) {
                System.err.println("range 1 error, key = " + Arrays.toString(key1));
            }
            long size1 = getSize(ranges1);
            size1Sum += size1;
        }
        long endTime = System.currentTimeMillis();
        long time1 = endTime - startTime;

        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            byte[] key2 = keyGenerator2.toKey(stObjects.get(i).getLocation());
            ArrayList<Range<byte[]>> ranges2 = keyGenerator2.toKeyRanges(queries.get(i));
            if (!check(key2, ranges2)) {
                System.err.println("range 2 error, key = " + Arrays.toString(key2));
            }

            long size2 = getSize(ranges2);
            size2Sum += size2;
        }
        endTime = System.currentTimeMillis();
        long time2 = endTime - startTime;

        System.out.println(time1 + " " + time2);
        System.out.println(size1Sum + " " + size2Sum);
    }
}
