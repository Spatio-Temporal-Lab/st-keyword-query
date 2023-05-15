package com.START.STKQ.model;

import java.io.Serializable;

public class MBR implements Serializable {
    private final double maxLat;
    private final double minLat;
    private final double maxLon;
    private final double minLon;
    private static final long serialVersionUID = 6529685098267757692L;

    public MBR(double minLat, double maxLat, double minLgt, double maxLgt) {
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLon = minLgt;
        this.maxLon = maxLgt;
    }

    public MBR(Range<Location> range) {
        Location loc1 = range.getLow();
        Location loc2 = range.getHigh();
        this.minLat = loc1.getLat();
        this.maxLat = loc2.getLat();
        this.minLon = loc1.getLon();
        this.maxLon = loc2.getLon();
    }

    public double getMaxLatitude() {
        return maxLat;
    }

    public double getMinLatitude() {
        return minLat;
    }

    public double getMaxLongitude() {
        return maxLon;
    }

    public double getMinLongitude() {
        return minLon;
    }

    public Location getLeftUp() {
        return new Location(minLat, minLon);
    }

    public Location getRightDown() {
        return new Location(maxLat, maxLon);
    }

    public boolean check() {
        return minLon >= -180.0 && minLat >= -90.0 && maxLon <= 180.0 && maxLat <= 90.0;
    }

    public boolean intersects(MBR mbr) {
        return !(minLat > mbr.getMaxLatitude() || minLon > mbr.getMaxLongitude() ||
                maxLat < mbr.getMinLatitude() || maxLon < mbr.getMinLongitude());
    }

    public MBR getIntersection(MBR mbr) {
        return new MBR(
                Math.max(minLat, mbr.minLat),
                Math.min(maxLat, mbr.maxLat),
                Math.max(minLon, mbr.minLon),
                Math.min(maxLon, mbr.maxLon)
        );
    }

    public String toString() {
        return minLat + " " + maxLat + " " + minLon + " " + maxLon;
    }
}
