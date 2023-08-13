package org.urbcomp.startdb.stkq.model;

import java.io.Serializable;

public class Location implements Serializable {
    private final double lat;
    private final double lon;

    public Location(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public Location(String str) {
        String[] strings = str.split(" ");
        lat = Double.parseDouble(strings[0]);
        lon = Double.parseDouble(strings[1]);
    }

    public String toString() {
        return lat + " " + lon;
    }

    public double getLat() {
        return this.lat;
    }

    public double getLon() {
        return this.lon;
    }

    public boolean in(MBR mbr) {
        return lat >= mbr.getMinLat() && lat <= mbr.getMaxLat() &&
                lon >= mbr.getMinLon() && lon <= mbr.getMaxLon();
    }

    public boolean equals(Location other) {
        return lat == other.lat && lon == other.lon;
    }
}
