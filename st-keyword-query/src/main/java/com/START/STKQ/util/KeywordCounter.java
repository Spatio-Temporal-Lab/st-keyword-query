package com.START.STKQ.util;

import java.util.*;

public class KeywordCounter {
    private static final Map<String, Integer> count = new HashMap<>();
    private static Map<String, Integer> textToID = new HashMap<>();
    private static final ArrayList<String> keywords = new ArrayList<>();
    private static int ID = 0;

    public static int size() {
        return count.size();
    }

    public static void add(String keyword) {
        count.merge(keyword, 1, Integer::sum);
        if (!textToID.containsKey(keyword)) {
            textToID.put(keyword, ID++);
            keywords.add(keyword);
        }
    }

    public static int getCount(String keyword) {
        return count.get(keyword);
    }

    public static int getID(String keyword) {
        return textToID.get(keyword);
    }

    public static Map<String, Integer> getIDS() {
        return textToID;
    }

    public static void setTextToID(Map<String, Integer> t) {
        textToID = t;
    }

    public static ArrayList<String> getKeywords() {
        return keywords;
    }
}