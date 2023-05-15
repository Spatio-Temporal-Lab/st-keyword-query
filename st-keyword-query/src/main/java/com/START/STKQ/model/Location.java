package com.START.STKQ.model;

public class Location {
    private final double lat;
    private final double lon;

    public Location(double a, double b) {
        this.lat = a;
        this.lon = b;
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
        return lat >= mbr.getMinLatitude() && lat <= mbr.getMaxLatitude() &&
                lon >= mbr.getMinLongitude() && lon <= mbr.getMaxLongitude();
    }

    public boolean equals(Location other) {
        return lat == other.lat && lon == other.lon;
    }
}