package org.urbcomp.startdb.stkq.STILT;

public class QueryBox {
    int keyword;
    int xMin;
    int xMax;
    int yMin;
    int yMax;
    int tMin;
    int tMax;

    public QueryBox(int xMin, int xMax, int yMin, int yMax, int tMin, int tMax, int keyword) {
        this.keyword = keyword;
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
        this.tMin = tMin;
        this.tMax = tMax;
    }

    public QueryBox(QueryBox box) {
        this.keyword = box.keyword;
        this.xMin = box.xMin;
        this.xMax = box.xMax;
        this.yMin = box.yMin;
        this.yMax = box.yMax;
        this.tMin = box.tMin;
        this.tMax = box.tMax;
    }

    @Override
    public String toString() {
        return "QueryBox{" +
                "keyword=" + keyword +
                ", xMin=" + xMin +
                ", xMax=" + xMax +
                ", yMin=" + yMin +
                ", yMax=" + yMax +
                ", tMin=" + tMin +
                ", tMax=" + tMax +
                '}';
    }
}
