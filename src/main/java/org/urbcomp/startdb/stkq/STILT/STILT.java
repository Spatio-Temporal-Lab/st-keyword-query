package org.urbcomp.startdb.stkq.STILT;

import org.apache.commons.math3.util.Pair;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class STILT {
    Node root;

    public void insert(long key, long id) {
        if (root == null) {
            root = new Node();
            if ((key >> 63 & 1) == 0) {
                root.leftLength = 64;
                root.leftPath = key;
                root.left = new Leaf(id);
            } else {
                root.rightLength = 64;
                root.rightPath = key;
                root.right = new Leaf(id);
            }
            return;
        }
        insert(root, key, (byte) 63, id);
    }

    public List<Long> query(long key) {
        if (root == null) {
            return new ArrayList<>();
        }
        return query(root, key, (byte) 63);
    }

    public List<Long> query(QueryBox queryBox) {
        return query(root, queryBox, new QueryBox(0, 0, 0, 0, 0, 0, 0), 0, 0, 0, (byte) 63);
    }

    private List<Long> query(AbstractTreeNode now, long key, byte curPos) {
        if (now instanceof Leaf) {
            return now.getIds();
        }

        List<Long> result = new ArrayList<>();

        Pair<Byte, Long> matchResult;
        if ((key >> curPos & 1) == 0) {
            if (now.getLeftLength() != 0) {
                matchResult = match(now.getLeftPath(), key, now.getLeftLength(), curPos);
                if (matchResult.getKey() == now.getLeftLength()) {
                    result.addAll(query(now.getLeft(), key, (byte) (curPos - now.getLeftLength())));
                }
            }
        } else {
            if (now.getRightLength() != 0) {
                matchResult = match(now.getRightPath(), key, now.getRightLength(), curPos);
                if (matchResult.getKey() == now.getRightLength()) {
                    result.addAll(query(now.getRight(), key, (byte) (curPos - now.getRightLength())));
                }
            }
        }

        return result;
    }

    private List<Long> query(AbstractTreeNode now, QueryBox boundary, QueryBox pre,
                             int xNow, int yNow, int tNow, byte curPos) {
        if (now instanceof Leaf) {
            return now.getIds();
        }

        List<Long> result = new ArrayList<>();
        QueryBox box = new QueryBox(pre);

        // keyword, must equal
        if ((curPos & 3) == 1) {
            int keywordBit = boundary.keyword >> (curPos >> 2) & 1;
            if (keywordBit == 0) {
                int lLen = now.getLeftLength();
                if (lLen == 0) {
                    return result;
                }
                long lPath = now.getLeftPath();
                for (int i = lLen - 1; i >= 0; --i, --curPos) {
                    byte p = (byte) (curPos >> 2);    //The bit position corresponding to x, y, t or keyword
                    int pathBit = (int) (lPath >> i & 1);
                    switch (curPos & 3) {
                        case 3:
                            xNow = xNow << 1 | pathBit;
                            box.xMin = box.xMin << 1 | (boundary.xMin >> p & 1);
                            if (xNow < boundary.xMin) {
                                return result;
                            }
                            box.xMax = box.xMax << 1 | (boundary.xMax >> p & 1);
                            if (xNow > boundary.xMax) {
                                return result;
                            }
                            break;
                        case 2:
                            yNow = yNow << 1 | pathBit;
                            box.yMin = box.yMin << 1 | (boundary.yMin >> p & 1);
                            if (yNow < boundary.yMin) {
                                return result;
                            }
                            box.yMax = box.yMax << 1 | (boundary.yMax >> p & 1);
                            if (yNow > boundary.yMax) {
                                return result;
                            }
                            break;
                        case 1:
                            if ((boundary.keyword >> p & 1) != pathBit) {
                                return result;
                            }
                            break;
                        case 0:
                            tNow = tNow << 1 | pathBit;
                            box.tMin = box.tMin << 1 | (boundary.tMin >> p & 1);
                            if (tNow < boundary.tMin) {
                                return result;
                            }
                            box.tMax = box.tMax << 1 | (boundary.tMax >> p & 1);
                            if (tNow > boundary.tMax) {
                                return result;
                            }
                            break;
                    }
                }
                result.addAll(query(now.getLeft(), boundary, box, xNow, yNow, tNow, curPos));
            } else {
                int rLen = now.getRightLength();
                if (rLen == 0) {
                    return result;
                }
                long rPath = now.getRightPath();
                for (int i = rLen - 1; i >= 0; --i, --curPos) {
                    byte p = (byte) (curPos >> 2);    //The bit position corresponding to x, y, t or keyword
                    int pathBit = (int) (rPath >> i & 1);
                    switch (curPos & 3) {
                        case 3:
                            xNow = xNow << 1 | pathBit;
                            box.xMin = box.xMin << 1 | (boundary.xMin >> p & 1);
                            if (xNow < boundary.xMin) {
                                return result;
                            }
                            box.xMax = box.xMax << 1 | (boundary.xMax >> p & 1);
                            if (xNow > boundary.xMax) {
                                return result;
                            }
                            break;
                        case 2:
                            yNow = yNow << 1 | pathBit;
                            box.yMin = box.yMin << 1 | (boundary.yMin >> p & 1);
                            if (yNow < boundary.yMin) {
                                return result;
                            }
                            box.yMax = box.yMax << 1 | (boundary.yMax >> p & 1);
                            if (yNow > boundary.yMax) {
                                return result;
                            }
                            break;
                        case 1:
                            if ((boundary.keyword >> p & 1) != pathBit) {
                                return result;
                            }
                            break;
                        case 0:
                            tNow = tNow << 1 | pathBit;
                            box.tMin = box.tMin << 1 | (boundary.tMin >> p & 1);
                            if (tNow < boundary.tMin) {
                                return result;
                            }
                            box.tMax = box.tMax << 1 | (boundary.tMax >> p & 1);
                            if (tNow > boundary.tMax) {
                                return result;
                            }
                            break;
                    }
                }
                result.addAll(query(now.getRight(), boundary, box, xNow, yNow, tNow, curPos));
            }
        }
        // range, must in boundary
        else {
            int lLen = now.getLeftLength();
            int oldX = xNow;
            int oldY = yNow;
            int oldT = tNow;
            byte oldPos = curPos;

            if (lLen != 0) {
                long lPath = now.getLeftPath();
                for (int i = lLen - 1; i >= 0; --i, --curPos) {
                    byte p = (byte) (curPos >> 2);    //The bit position corresponding to x, y, t or keyword
                    int pathBit = (int) (lPath >> i & 1);
                    switch (curPos & 3) {
                        case 3:
                            xNow = xNow << 1 | pathBit;
                            box.xMin = box.xMin << 1 | (boundary.xMin >> p & 1);
                            if (xNow < boundary.xMin) {
                                return result;
                            }
                            box.xMax = box.xMax << 1 | (boundary.xMax >> p & 1);
                            if (xNow > boundary.xMax) {
                                return result;
                            }
                            break;
                        case 2:
                            yNow = yNow << 1 | pathBit;
                            box.yMin = box.yMin << 1 | (boundary.yMin >> p & 1);
                            if (yNow < boundary.yMin) {
                                return result;
                            }
                            box.yMax = box.yMax << 1 | (boundary.yMax >> p & 1);
                            if (yNow > boundary.yMax) {
                                return result;
                            }
                            break;
                        case 1:
                            if ((boundary.keyword >> p & 1) != pathBit) {
                                return result;
                            }
                            break;
                        case 0:
                            tNow = tNow << 1 | pathBit;
                            box.tMin = box.tMin << 1 | (boundary.tMin >> p & 1);
                            if (tNow < boundary.tMin) {
                                return result;
                            }
                            box.tMax = box.tMax << 1 | (boundary.tMax >> p & 1);
                            if (tNow > boundary.tMax) {
                                return result;
                            }
                            break;
                    }
                }
                result.addAll(query(now.getLeft(), boundary, box, xNow, yNow, tNow, curPos));
            }

            int rLen = now.getRightLength();
            if (rLen != 0) {
                xNow = oldX;
                yNow = oldY;
                tNow = oldT;
                box = new QueryBox(pre);
                curPos = oldPos;

                long rPath = now.getRightPath();
                for (int i = rLen - 1; i >= 0; --i, --curPos) {
                    byte p = (byte) (curPos >> 2);    //The bit position corresponding to x, y, t or keyword
                    int pathBit = (int) (rPath >> i & 1);
                    switch (curPos & 3) {
                        case 3:
                            xNow = xNow << 1 | pathBit;
                            box.xMin = box.xMin << 1 | (boundary.xMin >> p & 1);
                            if (xNow < boundary.xMin) {
                                return result;
                            }
                            box.xMax = box.xMax << 1 | (boundary.xMax >> p & 1);
                            if (xNow > boundary.xMax) {
                                return result;
                            }
                            break;
                        case 2:
                            yNow = yNow << 1 | pathBit;
                            box.yMin = box.yMin << 1 | (boundary.yMin >> p & 1);
                            if (yNow < boundary.yMin) {
                                return result;
                            }
                            box.yMax = box.yMax << 1 | (boundary.yMax >> p & 1);
                            if (yNow > boundary.yMax) {
                                return result;
                            }
                            break;
                        case 1:
                            if ((boundary.keyword >> p & 1) != pathBit) {
                                return result;
                            }
                            break;
                        case 0:
                            tNow = tNow << 1 | pathBit;
                            box.tMin = box.tMin << 1 | (boundary.tMin >> p & 1);
                            if (tNow < boundary.tMin) {
                                return result;
                            }
                            box.tMax = box.tMax << 1 | (boundary.tMax >> p & 1);
                            if (tNow > boundary.tMax) {
                                return result;
                            }
                            break;
                    }
                }
                result.addAll(query(now.getRight(), boundary, box, xNow, yNow, tNow, curPos));
            }
        }

        return result;
    }

    private Pair<Byte, Long> match(long path, long key, byte pathLength, byte curPos) {
        byte i = (byte) (pathLength - 1);
        long commonPrefix = 0;
        for ( ; i >= 0; --i, --curPos) {
            if ((path >> i & 1) == (key >> curPos & 1)) {
                commonPrefix = commonPrefix << 1 | (path >> i & 1);
            } else {
                break;
            }
        }
        return new Pair<>((byte) (pathLength - i - 1), commonPrefix);
    }

    private Node splitLeft(Node now, Pair<Byte, Long> matchResult) {
        byte oldLeftLength = now.leftLength;
        long oldLeftPath = now.leftPath;
        AbstractTreeNode oldLeft = now.left;

        byte matchLength = matchResult.getKey();

        now.leftPath = matchResult.getValue();
        now.leftLength = matchLength;

        Node next = new Node();
        byte newPathLength = (byte) (oldLeftLength - matchLength);
        long newPath = oldLeftPath & ((1L << newPathLength) - 1);

        // origin child move to left
        if ((newPath >> newPathLength & 1) == 0) {
            next.left = oldLeft;
            next.leftPath = newPath;
            next.leftLength = newPathLength;
        } else {  // origin child move to right
            next.right = oldLeft;
            next.rightPath = newPath;
            next.rightLength = newPathLength;
        }

        now.left = next;
        return next;
    }

    private Node splitRight(Node now, Pair<Byte, Long> matchResult) {
        byte oldRightLength = now.rightLength;
        long oldRightPath = now.rightPath;
        AbstractTreeNode oldRight = now.right;

        byte matchLength = matchResult.getKey();

        now.rightPath = matchResult.getValue();
        now.rightLength = matchLength;

        Node next = new Node();
        byte newPathLength = (byte) (oldRightLength - matchLength);
        long newPath = oldRightPath & ((1L << newPathLength) - 1);

        // origin child move to left
        if ((newPath >> newPathLength & 1) == 0) {
            next.left = oldRight;
            next.leftPath = newPath;
            next.leftLength = newPathLength;
        } else {  // origin child move to right
            next.right = oldRight;
            next.rightPath = newPath;
            next.rightLength = newPathLength;
        }

        now.right = next;
        return next;
    }

    private void insert(AbstractTreeNode now, long key, byte curPos, long id) {
        if (now instanceof Leaf) {
            ((Leaf) now).add(id);
            return;
        }
        Pair<Byte, Long> matchResult;

        if ((key >> curPos & 1) == 0) {  // match left child
            // has no left child
            if (now.getLeftLength() == 0) {
                if (now == root) {
                    now.setLeftPath(key);
                } else {
                    now.setLeftPath(key & ((1L << (curPos + 1)) - 1));
                }
                now.setLeftLength((byte) (curPos + 1));
                now.setLeft(new Leaf(id));
            } else {
                matchResult = match(now.getLeftPath(), key, now.getLeftLength(), curPos);
                if (matchResult.getKey() == now.getLeftLength()) {  // match perfectly
                    insert(now.getLeft(), key, (byte) (curPos - now.getLeftLength()), id);
                } else {   // need to split the node
                    insert(splitLeft((Node) now, matchResult), key, (byte) (curPos - matchResult.getKey()), id);
                }
            }
        } else {  // match right child
            // has no right child
            if (now.getRightLength() == 0) {
                if (now == root) {
                    now.setRightPath(key);
                } else {
                    now.setRightPath(key & ((1L << (curPos + 1)) - 1));
                }
                now.setRightLength((byte) (curPos + 1));
                now.setRight(new Leaf(id));
            } else {
                matchResult = match(now.getRightPath(), key, now.getRightLength(), curPos);
                if (matchResult.getKey() == now.getRightLength()) {  // match perfectly
                    insert(now.getRight(), key, (byte) (curPos - now.getRightLength()), id);
                } else {   // need to split the node
                    insert(splitRight((Node) now, matchResult), key, (byte) (curPos - matchResult.getKey()), id);
                }
            }
        }

    }

    public void print() {
        AbstractTreeNode now = root;
        Queue<AbstractTreeNode> queue = new LinkedList<>();
        queue.add(now);
        int depth = 0;
        while (!queue.isEmpty()) {
            System.out.println("**************** the " + depth + " level:");
            Queue<AbstractTreeNode> queue1 = new LinkedList<>();
            while (!queue.isEmpty()) {
                AbstractTreeNode node = queue.poll();
                if (node instanceof Leaf) {
                    System.out.println("ids: " + node.getIds());
                } else {
                    if (node.getLeftLength() > 0) {
                        System.out.println("l: " + node.getLeftLength()  + " " + node.getLeftPath());
                        queue1.add(node.getLeft());
                    }
                    if (node.getRightLength() > 0) {
                        System.out.println("r: " + node.getRightLength()  + " " +  node.getRightPath());
                        queue1.add(node.getRight());
                    }
                }
            }
            ++depth;
            queue = new LinkedList<>(queue1);
        }
    }

    public static void main(String[] args) {
        STILT tree = new STILT();
        long begin = System.currentTimeMillis();
        int testCount = 256 * 1024 * 1024;
        for (int i = 0; i < testCount; ++i) {
            tree.insert(i, i);
        }
        long end = System.currentTimeMillis();
        System.out.println((end - begin) + "ms");

//        tree.print();
        System.out.println(RamUsageEstimator.humanSizeOf(tree));

        begin = System.currentTimeMillis();

        for (int i = 0; i < 4; ++i) {
            System.out.println(tree.query(new QueryBox(
                    0, 3, 0, 3, 0, 3, i
                ))
            );
        }

        end = System.currentTimeMillis();
        System.out.println((end - begin) + "ms");
    }
}
