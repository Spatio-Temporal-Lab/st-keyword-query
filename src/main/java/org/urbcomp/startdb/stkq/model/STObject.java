package org.urbcomp.startdb.stkq.model;

import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

public class STObject implements Serializable, Comparable<STObject> {
    private final Location place;
    private final Date date;
    private final ArrayList<String> keywords;
    private String sentence;

    private long ID;

    public STObject(long id, double lat, double lon, Date date, ArrayList<String> keywords) {
        this.ID = id;
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

    public STObject(String csvLine) throws ParseException {
        String[] items = csvLine.split(",");
        place = new Location(Double.parseDouble(items[0]), Double.parseDouble(items[1]));
        date = DateUtil.getDate(items[2]);
        keywords = new ArrayList<>();
        int n = items.length;
        keywords.addAll(Arrays.asList(items).subList(3, n));
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

    public String toVSCLine() {
        return place.getLat() + "," + place.getLon() + "," + DateUtil.format(date) + "," + String.join(",", keywords);
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


