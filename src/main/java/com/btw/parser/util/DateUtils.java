package com.btw.parser.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
* Created by ydc on 2019/11/19.
*/
public final class DateUtils {

    public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat SDF_HM = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    public static final SimpleDateFormat SDF_HMS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public DateUtils() {
    }

    public static String getCurrentDay(Date date) {
        return SDF.format(date);
    }

    public static int subDate(String d1, String d2) throws ParseException {
        Long diff = (SDF.parse(d1).getTime() - SDF.parse(d2).getTime()) / 1000/60/60/24;
        return diff.intValue();
    }

    public static boolean isBefore(String date1, String date2) throws ParseException {
        Date d1 = SDF.parse(date1);
        Date d2 = SDF.parse(date2);
        return d1.before(d2);
    }
    public static String getPNDay(String date, int delayD) throws ParseException {
        return getPNDay(SDF.parse(date), delayD);
    }

    public static String getPNDay(Date date, int delayD) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, delayD);
        return SDF.format(calendar.getTime());
    }
}
