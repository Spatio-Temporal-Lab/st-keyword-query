package com.START.STKQ.util;

import junit.framework.TestCase;

import java.text.ParseException;
import java.util.Date;

public class DateUtilTest extends TestCase {

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

    public void testGetDateAfterHours() {
        for (int i = 0; i < 25; ++i) {
            System.out.println(DateUtil.getDateAfter(i));
        }
    }

    public void testLastDayOfMonth() throws ParseException {
        Date date = DateUtil.getDate("2000-02-01 00:00:00");
        System.out.println(DateUtil.lastDayOfMonth(date));
        date = DateUtil.getDate("2001-02-01 00:00:00");
        System.out.println(DateUtil.lastDayOfMonth(date));
    }

    public void testFirstDayOfMonth() throws ParseException {
        Date date = DateUtil.getDate("2000-02-01 00:00:00");
        System.out.println(DateUtil.firstDayOfMonth(date));
        date = DateUtil.getDate("2001-02-01 00:00:00");
        System.out.println(DateUtil.firstDayOfMonth(date));
        date.setMonth(0);
        System.out.println(date);
    }
}