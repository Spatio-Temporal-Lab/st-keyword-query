package org.urbcomp.startdb.stkq.model;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.util.ArrayList;
import java.util.Date;

public class Query {
    private Date startTime;
    private Date endTime;
    private MBR mbr;
    private ArrayList<String> keywords;
    private QueryType queryType;

    public Query() {}

    public Query(double minLat, double maxLat,
                 double minLon, double maxLon,
                 Date startTime, Date endTime, ArrayList<String> keywords) {
        this.keywords = new ArrayList<>(keywords);
        this.startTime = startTime;
        this.endTime = endTime;
        this.mbr = new MBR(minLat, maxLat, minLon, maxLon);
    }

    public Query(MBR mbr, Date startTime, Date endTime, ArrayList<String> keywords) {
        this.keywords = new ArrayList<>(keywords);
        this.startTime = startTime;
        this.endTime = endTime;
        this.mbr = mbr;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public double getMinLon() {
        return mbr.getMinLon();
    }

    public double getMinLat() {
        return mbr.getMinLat();
    }

    public double getMaxLat() {
        return mbr.getMaxLat();
    }

    public double getMaxLon() {
        return mbr.getMaxLon();
    }

    public ArrayList<String> getKeywords() {
        return keywords;
    }

    public MBR getMBR() {
        return mbr;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setMbr(MBR mbr) {
        this.mbr = mbr;
    }

    public void setKeywords(ArrayList<String> keywords) {
        this.keywords = keywords;
    }

    public String toString() {
        return mbr.toString() + " " + DateUtil.format(startTime) + " "
                + DateUtil.format(endTime) + " " + keywords;
    }
}
