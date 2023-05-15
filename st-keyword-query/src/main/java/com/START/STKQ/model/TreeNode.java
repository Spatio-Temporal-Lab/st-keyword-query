package com.START.STKQ.model;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.Serializable;

public class TreeNode implements Serializable {
    private static final long serialVersionUID = 6529685098267757691L;
    private final TreeNode[] children;
    private final MBR mbr;
    private final double midLat;
    private final double midLon;
    private final double minLat;
    private final double minLon;
    private final double maxLat;
    private final double maxLon;
    private boolean isLeaf;
    private int count;

    protected TreeNode(MBR mbr) {
        this.mbr = mbr;
        minLat = mbr.getMinLatitude();
        minLon = mbr.getMinLongitude();
        maxLat = mbr.getMaxLatitude();
        maxLon = mbr.getMaxLongitude();
        midLat = (minLat + maxLat) / 2;
        midLon = (minLon + maxLon) / 2;
        children = new TreeNode[4];
        isLeaf = true;
        count = 0;
    }

    public int getCount() {
        return count;
    }

    public TreeNode[] getChildren() {
        return children;
    }

    public TreeNode getChild(int index) {
        return children[index];
    }

    public TreeNode getChild(double lat, double lon) {
        if (isLeaf) {
            System.err.println("leaf nodes have no child!");
            return null;
        }

        if (lat < midLat && lon <= midLon) {
            if (children[0] == null) {
                children[0] = new LeafNode(BloomFilter.create(Funnels.byteArrayFunnel(), 3240, 0.001), new MBR(minLat, midLat, minLon, midLon));
            }
            return children[0];
        } else if (lat <= midLat && lon > midLon) {
            if (children[1] == null) {
                children[1] = new LeafNode(BloomFilter.create(Funnels.byteArrayFunnel(), 3240, 0.001),  new MBR(minLat, midLat, midLon, maxLon));
            }
            return children[1];
        } else if (lat >= midLat && lon < midLon) {
            if (children[2] == null) {
                children[2] = new LeafNode(BloomFilter.create(Funnels.byteArrayFunnel(), 3240, 0.001),  new MBR(midLat, maxLat, minLon, midLon));
            }
            return children[2];
        }
        if (children[3] == null) {
            children[3] = new LeafNode(BloomFilter.create(Funnels.byteArrayFunnel(), 3240, 0.001), new MBR(midLat, maxLat, midLon, maxLon));
        }
        return children[3];
    }

    void setChild(TreeNode node, int index) {
        children[index] = node;
    }

    public MBR getMbr() {
        return mbr;
    }

    public double getMidLat() {
        return midLat;
    }

    public double getMidLon() {
        return midLon;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getMaxLat() {
        return mbr.getMaxLatitude();
    }

    public double getMinLat() {
        return mbr.getMinLatitude();
    }

    public double getMaxLon() {
        return mbr.getMaxLongitude();
    }

    public double getMinLon() {
        return mbr.getMinLongitude();
    }

    public void add(byte[] code) {}
}
