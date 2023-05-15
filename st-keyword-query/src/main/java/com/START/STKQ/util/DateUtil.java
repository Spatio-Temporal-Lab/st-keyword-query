package com.START.STKQ.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Date minDate;
    private static final long minTimeMill;

    static {
        try {
            minDate = DateUtil.getDate("2000-01-01 00:00:00");
            minTimeMill = minDate.getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String format(Date date) {
        return sdf.format(date);
    }

    public static Date getDate(String s) throws ParseException {
        return sdf.parse(s);
    }

    public static Date getDateAfter(Date date, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, minute);
        return calendar.getTime();
    }

    public static int getHours(Date date1, Date date2) {
        long from = date1.getTime();
        long to = date2.getTime();
        return (int) ((to - from) / (1000 * 60 * 60));
    }

    public static int getHours(Date date) {
        return (int) ((date.getTime() - minTimeMill) / (1000 * 60 * 60));
    }

    public static Date lastDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.roll(Calendar.DAY_OF_MONTH, -1);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    public static Date firstDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    //[l, r) [s, t]
    public static boolean intersects(Date l, Date r, Date s, Date t) {
        return (t.after(l) || t.equals(l)) && s.before(r);
    }
}