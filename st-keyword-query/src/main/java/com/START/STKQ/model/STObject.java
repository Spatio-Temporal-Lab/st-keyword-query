package com.START.STKQ.model;

import com.START.STKQ.util.DateUtil;

import java.io.Serializable;
import java.util.*;

public class STObject implements Serializable, Comparable<STObject> {
    private final Location place;
    private final Date date;
    private final ArrayList<String> keywords;
    private final String sentence;

    private final long ID;

    public STObject(long id, double lat, double lon, Date date, ArrayList<String> keywords) {
        ID = id;
        this.place = new Location(lat, lon);
        this.date = date;
        this.keywords = new ArrayList<>(keywords);
        this.sentence = String.join(" ", keywords);
    }

    public STObject(long id, double lat, double lon, Date date, String[] keywords) {
        ID = id;
        this.place = new Location(lat, lon);
        this.date = date;
        this.keywords = new ArrayList<>(Arrays.asList(keywords));
        this.sentence = String.join(" ", keywords);
    }

    public double getLat() {
        return this.place.getLat();
    }

    public double getLon() {
        return this.place.getLon();
    }

    public Location getLocation() {
        return place;
    }

    public Date getDate() {
        return this.date;
    }

    public Location getPlace() {
        return place;
    }

    public long getID() {
        return ID;
    }

    public double keywordCounts() {
        return this.keywords.size();
    }

    public ArrayList<String> getKeywords() {
        return this.keywords;
    }

    public String getSentence() {
        return sentence;
    }

    public String toString() {
        return ID + " " + place.toString() + " " + DateUtil.format(date) + " " + keywords;
    }

    public boolean equals(STObject other) {
        if (!(place.equals(other.place) && date.equals(other.date))) {
            return false;
        }
        Set<String> s1 = new HashSet<>(keywords);
        Set<String> s2 = new HashSet<>(other.keywords);
        if (s1.size() != s2.size()) {
            return false;
        }
        for (String s : s1) {
            if (!s2.contains(s)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(STObject stObject) {
        return Long.compare(ID, stObject.getID());
    }
}


