package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class STKUtil {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static boolean checkWords(List<String> oKeywords, List<String> qKeywords, QueryType queryType) {
        if (queryType.equals(QueryType.CONTAIN_ONE)) {
            for (String s : qKeywords) {
                if (oKeywords.contains(s)) {
                    return true;
                }
            }
            return false;
        } else if (queryType.equals(QueryType.CONTAIN_ALL)) {
            for (String s : qKeywords) {
                if (!oKeywords.contains(s)) {
                    return false;
                }
            }
            return true;
        } else {
            //TODO support more query
            return false;
        }
    }

    public static boolean check(STObject object, Query query) {
        Location location = object.getLocation();
        if (!location.in(query.getMBR())) {
            return false;
        }
        Date time = object.getTime();
        if (time.before(query.getStartTime()) || time.after(query.getEndTime())) {
            return false;
        }
        return checkWords(object.getKeywords(), query.getKeywords(), query.getQueryType());
    }

    public static boolean check(Map<String, String> map, Query query) {
        Location loc = new Location(map.get("loc"));
        if (!loc.in(query.getMBR())) {
            return false;
        }

        Date date;
        try {
            synchronized (sdf) {
                date = sdf.parse(map.get("time"));
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (date.before(query.getStartTime()) || date.after(query.getEndTime())) {
            return false;
        }
        ArrayList<String> keywords = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));
        return checkWords(keywords, query.getKeywords(), query.getQueryType());
    }
}
