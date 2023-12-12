package org.urbcomp.startdb.stkq.model;

import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

public class STObject implements Serializable, Comparable<STObject> {
    private final Location location;
    private final Date time;
    private final List<String> keywords;

    private long ID;

    public STObject(long id, double lat, double lon, Date time, List<String> keywords) {
        this.ID = id;
        this.location = new Location(lat, lon);
        this.time = time;
        this.keywords = new ArrayList<>(keywords);
    }

    public STObject(long id, double lat, double lon, Date time, String[] keywords) {
        this.ID = id;
        this.location = new Location(lat, lon);
        this.time = time;
        this.keywords = new ArrayList<>(Arrays.asList(keywords));
    }

    public STObject(String csvLine) throws ParseException {
        String[] items = csvLine.split(",");
        location = new Location(Double.parseDouble(items[0]), Double.parseDouble(items[1]));
        time = DateUtil.getDate(items[2]);
        keywords = new ArrayList<>();
        keywords.addAll(Arrays.asList(items).subList(3, items.length));
    }

    public double getLat() {
        return this.location.getLat();
    }

    public double getLon() {
        return this.location.getLon();
    }

    public Location getLocation() {
        return location;
    }

    public Date getTime() {
        return this.time;
    }

    public long getID() {
        return ID;
    }

    public List<String> getKeywords() {
        return this.keywords;
    }

    public String getSentence() {
        return String.join(" ", keywords);
    }

    public String toString() {
        return ID + " " + location.toString() + " " + DateUtil.format(time) + " " + keywords;
    }

    public String toVSCLine() {
        return location.getLat() + "," + location.getLon() + "," + DateUtil.format(time) + "," + String.join(",", keywords);
    }

    public boolean equals(STObject other) {
        if (!(location.equals(other.location) && time.equals(other.time))) {
            System.out.println("location not equal or time not equal");
            System.out.println(this);
            System.out.println(other);
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
        if (!time.equals(stObject.time)) {
            return time.compareTo(stObject.time);
        }
        Location loc1 = stObject.getLocation();
        if (loc1.getLat() != location.getLat()) {
            return Double.compare(location.getLat(), loc1.getLat());
        }
        if (loc1.getLon() != location.getLon()) {
            return Double.compare(location.getLon(), loc1.getLon());
        }
        return String.join(" ", keywords).compareTo(String.join(" ", stObject.getKeywords()));
    }

    public void setID(int id) {
        ID = id;
    }
}


