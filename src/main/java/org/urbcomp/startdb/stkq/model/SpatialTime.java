package org.urbcomp.startdb.stkq.model;

import java.util.Date;

public class SpatialTime {
    private Date date;
    private Location location;

    public SpatialTime(Date date, Location location) {
        this.date = date;
        this.location = location;
    }

    public Date getDate() {
        return date;
    }

    public Location getLocation() {
        return location;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "SpatialTime{" +
                "date=" + date +
                ", location=" + location +
                '}';
    }
}
