package com.START.STKQ.model;

import java.io.Serializable;
import java.util.ArrayList;

public class NodeWithObjects extends TreeNode implements Serializable {
    private final ArrayList<STObject> objects;
    private final NodeWithObjects[] children;

    public NodeWithObjects(MBR mbr) {
        super(mbr);
        objects = new ArrayList<>();
        children = new NodeWithObjects[4];
    }

    public NodeWithObjects(MBR mbr, ArrayList<STObject> objects) {
        super(mbr);
        this.objects = new ArrayList<>(objects);
        children = new NodeWithObjects[4];
    }

    @Override
    public NodeWithObjects[] getChildren() {
        return children;
    }

    public void insert(STObject object) {
        setCount(getCount() + 1);
        objects.add(object);
    }

    public void insert(ArrayList<STObject> objects) {
        setCount(getCount() + objects.size());
        this.objects.addAll(objects);
    }

    public ArrayList<STObject> getObjects() {
        return objects;
    }

    void clear() {
        objects.clear();
    }
}