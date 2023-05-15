package com.START.STKQ.exp;

import com.START.STKQ.io.DataReader;
import com.START.STKQ.keyGenerator.HilbertSpatialKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialKeyGenerator;
import com.START.STKQ.model.MBR;
import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.model.STObject;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.GeoUtil;

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

        DataReader dataReader = new DataReader();
        dataReader.setLimit(10_00000);
        ArrayList<STObject> stObjects = dataReader.getSTObjects("E:\\data\\tweet\\tweet_2.csv");

        long size1Sum = 0;
        long size2Sum = 0;

        ArrayList<Query> queries = new ArrayList<>();
        for (STObject object : stObjects) {
            MBR mbr = GeoUtil.getMBRByCircle(object.getPlace(), 1000);
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