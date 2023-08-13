package org.urbcomp.startdb.stkq.model;

import java.io.Serializable;

public class MBR implements Serializable {
    private final double maxLat;
    private final double minLat;
    private final double maxLon;
    private final double minLon;
    private static final long serialVersionUID = 6529685098267757692L;

    public MBR(double minLat, double maxLat, double minLon, double maxLon) {
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLon = minLon;
        this.maxLon = maxLon;
    }

    public double getMaxLat() {
        return maxLat;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public double getMinLon() {
        return minLon;
    }

    public boolean check() {
        return minLon >= -180.0 && minLat >= -90.0 && maxLon <= 180.0 && maxLat <= 90.0;
    }

    public String toString() {
        return minLat + " " + maxLat + " " + minLon + " " + maxLon;
    }
}
