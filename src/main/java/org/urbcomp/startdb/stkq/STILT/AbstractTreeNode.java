package org.urbcomp.startdb.stkq.STILT;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTreeNode {
    public abstract long getLeftPath();

    public abstract void setLeftPath(long leftPath);

    public abstract byte getLeftLength();

    public abstract void setLeftLength(byte leftLength);

    public abstract long getRightPath();

    public abstract void setRightPath(long rightPath);

    public abstract byte getRightLength();

    public abstract void setRightLength(byte rightLength);

    public abstract AbstractTreeNode getLeft();

    public abstract void setLeft(AbstractTreeNode left);

    public abstract AbstractTreeNode getRight();

    public abstract void setRight(AbstractTreeNode right);

    public abstract List<Long> getIds();
}

class Node extends AbstractTreeNode {
    long leftPath;
    byte leftLength;
    long rightPath;
    byte rightLength;
    AbstractTreeNode left;
    AbstractTreeNode right;

    @Override
    public long getLeftPath() {
        return leftPath;
    }

    @Override
    public void setLeftPath(long leftPath) {
        this.leftPath = leftPath;
    }

    @Override
    public byte getLeftLength() {
        return leftLength;
    }

    @Override
    public void setLeftLength(byte leftLength) {
        this.leftLength = leftLength;
    }

    @Override
    public long getRightPath() {
        return rightPath;
    }

    @Override
    public void setRightPath(long rightPath) {
        this.rightPath = rightPath;
    }

    @Override
    public byte getRightLength() {
        return rightLength;
    }

    @Override
    public void setRightLength(byte rightLength) {
        this.rightLength = rightLength;
    }

    @Override
    public AbstractTreeNode getLeft() {
        return left;
    }

    @Override
    public void setLeft(AbstractTreeNode left) {
        this.left = left;
    }

    @Override
    public AbstractTreeNode getRight() {
        return right;
    }

    @Override
    public void setRight(AbstractTreeNode right) {
        this.right = right;
    }

    @Override
    public List<Long> getIds() {
        return null;
    }
}

class Leaf extends AbstractTreeNode {
    List<Long> idList = new ArrayList<>();
    Leaf(long id) {
        idList.add(id);
    }

    void add(long id) {
        idList.add(id);
    }

    @Override
    public long getLeftPath() {
        return 0;
    }

    @Override
    public void setLeftPath(long leftPath) {

    }

    @Override
    public byte getLeftLength() {
        return 0;
    }

    @Override
    public void setLeftLength(byte leftLength) {

    }

    @Override
    public long getRightPath() {
        return 0;
    }

    @Override
    public void setRightPath(long rightPath) {

    }

    @Override
    public byte getRightLength() {
        return 0;
    }

    @Override
    public void setRightLength(byte rightLength) {

    }

    @Override
    public AbstractTreeNode getLeft() {
        return null;
    }

    @Override
    public void setLeft(AbstractTreeNode left) {

    }

    @Override
    public AbstractTreeNode getRight() {
        return null;
    }

    @Override
    public void setRight(AbstractTreeNode right) {

    }

    @Override
    public List<Long> getIds() {
        return idList;
    }
}
