package org.urbcomp.startdb.stkq.model;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.util.ArrayList;
import java.util.Date;

public class Query {
    private Date s;
    private Date t;
    private MBR mbr;
    private ArrayList<String> keywords;
    private QueryType queryType;

    public Query(double lat1, double lat2, double lon1, double lon2, Date s, Date t, ArrayList<String> keywords) {
        this.keywords = new ArrayList<>(keywords);
        this.s = s;
        this.t = t;
        this.mbr = new MBR(lat1, lat2, lon1, lon2);
    }

    public Query(double lat1, double lat2, double lon1, double lon2, Date s, Date t, String keyword) {
        this.keywords = new ArrayList<>();
        this.keywords.add(keyword);
        this.s = s;
        this.t = t;
        this.mbr = new MBR(lat1, lat2, lon1, lon2);
    }

    public Query(MBR mbr, Date s, Date t, ArrayList<String> keywords) {
        this.keywords = new ArrayList<>(keywords);
        this.s = s;
        this.t = t;
        this.mbr = mbr;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public Date getS() {
        return s;
    }

    public void setS(Date d) {
        s = d;
    }

    public void setT(Date d) {
        t = d;
    }

    public Date getT() {
        return t;
    }

    public double getLeft() {
        return mbr.getMinLongitude();
    }

    public double getUp() {
        return mbr.getMinLatitude();
    }

    public double getDown() {
        return mbr.getMaxLatitude();
    }

    public double getRight() {
        return mbr.getMaxLongitude();
    }

    public ArrayList<String> getKeywords() {
        return keywords;
    }

    public MBR getMBR() {
        return mbr;
    }

    public void setMbr(MBR mbr) {
        this.mbr = mbr;
    }

    public String toString() {
        return mbr.toString() + " " + DateUtil.format(s) + " "
                + DateUtil.format(t) + " " + keywords;
    }

    public void setKeywords(ArrayList<String> keywords) {
        this.keywords = keywords;
    }
}
