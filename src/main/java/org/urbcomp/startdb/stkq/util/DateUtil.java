package org.urbcomp.startdb.stkq.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Date referenceDate;

    static {
        try {
            referenceDate = DateUtil.getDate("2000-01-01 00:00:00");
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

    public static Date getDateAfterMinutes(Date date, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, minute);
        return calendar.getTime();
    }

    public static Date getDateAfterHours(int hour) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(referenceDate);
        calendar.add(Calendar.HOUR_OF_DAY, hour);
        return calendar.getTime();
    }

    public static Date getDateAfterHours(Date date, int hour) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, hour);
        return calendar.getTime();
    }

    public static int getHours(Date date1, Date date2) {
        long from = date1.getTime();
        long to = date2.getTime();
        return (int) ((to - from) / (1000 * 60 * 60));
    }

    public static int getHours(Date date) {
        return getHours(referenceDate, date);
    }
}
