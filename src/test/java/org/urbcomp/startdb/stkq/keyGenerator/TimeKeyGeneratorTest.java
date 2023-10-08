package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;
import junit.framework.TestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

public class TimeKeyGeneratorTest extends TestCase {
    TimeKeyGeneratorNew timeKeyGenerator = new TimeKeyGeneratorNew();

    public void testToKey() throws ParseException {
        Date date = DateUtil.getDate("2000-01-01 00:00:00");
        for (int i = 0; i < 10; ++i) {
            assertArrayEquals(timeKeyGenerator.toBytes(date), ByteUtil.getKByte(i, 3));
            date = DateUtil.getDateAfterMinutes(date, 60);
        }
    }

    public void testToKeyRanges() throws ParseException {
        Date date1 = DateUtil.getDate("2000-01-01 00:00:00");
        Date date2 = DateUtil.getDate("2000-01-01 01:00:00");
        for (int i = 0; i < 10; ++i) {
            Query query = new Query(0.0, 0.0, 0.0, 0.0, date1, date2, new ArrayList<>());
            List<Range<byte[]>> ranges = timeKeyGenerator.toNumberRanges(query).stream().map(
                    range -> new Range<>(timeKeyGenerator.numberToBytes(range.getLow()), timeKeyGenerator.numberToBytes(range.getHigh()))
            ).collect(Collectors.toList());
            for (Range<byte[]> range : ranges) {
                assertArrayEquals(range.getLow(), ByteUtil.getKByte(i, 3));
                assertArrayEquals(range.getHigh(), ByteUtil.getKByte(i + 1, 3));
            }
            date1 = DateUtil.getDateAfterMinutes(date1, 60);
            date2 = DateUtil.getDateAfterMinutes(date2, 60);
        }
    }
}
