package org.urbcomp.startdb.stkq.util;

import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class DateUtilTest {

    @Test
    public void testGetHours() throws ParseException {
        //yyyy-MM-dd HH:mm
        Date date1 = DateUtil.getDate("2000-01-01 00:00:00");
        Date date2 = DateUtil.getDate("2000-01-01 01:01:00");
        Date date3 = DateUtil.getDate("2000-01-01 00:59:00");
        Date date4 = DateUtil.getDate("2000-01-02 00:00:00");
        assertEquals(1, DateUtil.getHours(date1, date2));
        assertEquals(0, DateUtil.getHours(date1, date3));
        assertEquals(24, DateUtil.getHours(date1, date4));
    }

    @Test
    public void testGetDateAfterHours() {
        for (int i = 0; i < 25; ++i) {
            System.out.println(DateUtil.getDateAfterHours(i));
        }
    }
}
