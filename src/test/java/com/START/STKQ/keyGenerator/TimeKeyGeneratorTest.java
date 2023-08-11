package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;
import junit.framework.TestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import static org.junit.Assert.assertArrayEquals;

public class TimeKeyGeneratorTest extends TestCase {
    TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();

    public void testToKey() throws ParseException {
        Date date = DateUtil.getDate("2000-01-01 00:00:00");
        for (int i = 0; i < 10; ++i) {
            assertArrayEquals(timeKeyGenerator.toKey(date), ByteUtil.getKByte(i, 3));
            date = DateUtil.getDateAfter(date, 60);
        }
    }

    public void testToKeyRanges() throws ParseException {
        Date date1 = DateUtil.getDate("2000-01-01 00:00:00");
        Date date2 = DateUtil.getDate("2000-01-01 01:00:00");
        for (int i = 0; i < 10; ++i) {
            Query query = new Query(0.0, 0.0, 0.0, 0.0, date1, date2, new ArrayList<>());
            ArrayList<Range<byte[]>> ranges = timeKeyGenerator.toKeyRanges(query);
            for (Range<byte[]> range : ranges) {
                assertArrayEquals(range.getLow(), ByteUtil.getKByte(i, 3));
                assertArrayEquals(range.getHigh(), ByteUtil.getKByte(i + 1, 3));
            }
            date1 = DateUtil.getDateAfter(date1, 60);
            date2 = DateUtil.getDateAfter(date2, 60);
        }
    }
}