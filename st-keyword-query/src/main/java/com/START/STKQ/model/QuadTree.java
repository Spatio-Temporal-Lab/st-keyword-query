package com.START.STKQ.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class QuadTree implements Serializable {
    private final NodeWithObjects root;
    private final int maxLevel;
    private final int maxCount;

    public QuadTree(int maxLevel, int maxCount) {
        this.maxLevel = maxLevel;
        this.maxCount = maxCount;
        root = new NodeWithObjects(new MBR(-90, 90, -180, 180));
    }

    private NodeWithObjects getNext(NodeWithObjects cur, STObject object) {
        double minLat = cur.getMinLat();
        double midLat = cur.getMidLat();
        double maxLat = cur.getMaxLat();
        double minLon = cur.getMinLon();
        double midLon = cur.getMidLon();
        double maxLon = cur.getMaxLon();
        double lat = object.getLat();
        double lon = object.getLon();
        TreeNode[] children = cur.getChildren();

        if (lat < midLat && lon <= midLon) {
            if (children[0] == null) {
                children[0] = new NodeWithObjects(new MBR(minLat, midLat, minLon, midLon));
            }
            return (NodeWithObjects) children[0];
        } else if (lat <= midLat && lon > midLon) {
            if (children[1] == null) {
                children[1] = new NodeWithObjects(new MBR(minLat, midLat, midLon, maxLon));
            }
            return (NodeWithObjects) children[1];
        } else if (lat >= midLat && lon < midLon) {
            if (children[2] == null) {
                children[2] = new NodeWithObjects(new MBR(midLat, maxLat, minLon, midLon));
            }
            return (NodeWithObjects) children[2];
        } else {
            if (children[3] == null) {
                children[3] = new NodeWithObjects(new MBR(midLat, maxLat, midLon, maxLon));
            }
            return (NodeWithObjects) children[3];
        }
    }

    private NodeWithObjects split(NodeWithObjects cur, STObject objectCur) {
        for (STObject object : cur.getObjects()) {
            NodeWithObjects next = getNext(cur, object);
            next.insert(object);
        }
        cur.setLeaf(false);
        cur.clear();
        return getNext(cur, objectCur);
    }

    public void build(ArrayList<STObject> objects) {
        for (STObject object : objects) {
            NodeWithObjects cur = root;
            int level = 0;
            boolean first = true;
            while (level < maxLevel) {
                if (!cur.isLeaf()) {
                    cur = getNext(cur, object);
                    ++level;
                } else {
                    if (first) {
                        cur.insert(object);
                        first = false;
                    }
                    if (level < maxLevel - 1 && cur.getCount() > maxCount) {
                        cur = split(cur, object);
                        ++level;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    public ArrayList<NodeWithObjects> getLeafs() {
        ArrayList<NodeWithObjects> leafs = new ArrayList<>();
        Queue<NodeWithObjects> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Queue<NodeWithObjects> childrenQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                NodeWithObjects cur = queue.poll();
                if (cur == null) {
                    continue;
                }
                if (cur.isLeaf()) {
                    leafs.add(cur);
                } else {
                    childrenQueue.addAll(Arrays.asList(cur.getChildren()));
                }
            }
            while (!childrenQueue.isEmpty()) {
                queue.add(childrenQueue.poll());
            }
        }
        return leafs;
    }

    public void print() {
        Queue<NodeWithObjects> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Queue<NodeWithObjects> childrenQueue = new LinkedList<>();
            System.out.println("*****************");
            while (!queue.isEmpty()) {
                NodeWithObjects cur = queue.poll();
                if (cur == null) {
                    continue;
                }
                if (cur.getObjects().size() != 0) {
                    System.out.println("------------");
                    System.out.println(cur.getObjects().size());
                    for (STObject object : cur.getObjects()) {
                        System.out.println(object);
                    }
                    System.out.println("------------");
                }
                childrenQueue.addAll(Arrays.asList(cur.getChildren()));
            }
            System.out.println("*****************");
            System.out.println("\n");
            while (!childrenQueue.isEmpty()) {
                queue.add(childrenQueue.poll());
            }
        }
    }

    public NodeWithObjects getRoot() {
        return root;
    }
}
