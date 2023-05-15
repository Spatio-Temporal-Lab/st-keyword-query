package com.START.STKQ.model;

import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.util.ByteUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.*;

public class STQuadTree extends AbstractSTKeyGenerator implements Serializable {
    private final TreeNode root;
    private AbstractSTKeyGenerator keyGenerator;
    private static final long serialVersionUID = 6529685098267757690L;

    public STQuadTree(NodeWithObjects otherRoot) {
        root = new TreeNode(otherRoot.getMbr());
        root.setLeaf(otherRoot.isLeaf());
//        root.setCount(otherRoot.getCount());

        Queue<NodeWithObjects> queue1 = new LinkedList<>();
        Queue<TreeNode> queue2 = new LinkedList<>();
        queue1.add(otherRoot);
        queue2.add(root);

        while (!queue1.isEmpty()) {
            Queue<NodeWithObjects> queue1Next = new LinkedList<>();
            Queue<TreeNode> queue2Next = new LinkedList<>();

            while (!queue1.isEmpty()) {
                NodeWithObjects otherNode = queue1.poll();
                TreeNode myNode = queue2.poll();

                if (otherNode == null || myNode == null) {
                    continue;
                }

                int i = 0;
                for (NodeWithObjects otherChild : otherNode.getChildren()) {
                    i++;
                    if (otherChild == null) {
                        continue;
                    }
                    TreeNode node;
                    if (otherChild.isLeaf()) {
                        //103991699/32105
                        node = new LeafNode(BloomFilter.create(Funnels.byteArrayFunnel(), 3240, 0.001), otherChild.getMbr());
                    } else {
                        node = new TreeNode(otherChild.getMbr());
                    }
                    node.setLeaf(otherChild.isLeaf());
//                    node.setCount(otherChild.getCount());

                    myNode.setChild(node, i - 1);

                    queue1Next.add(otherChild);
                    queue2Next.add(myNode.getChild(i - 1));
                }
            }

            queue1 = new LinkedList<>(queue1Next);
            queue2 = new LinkedList<>(queue2Next);
        }
    }

    public void setKeyGenerator(AbstractSTKeyGenerator keyGenerator) {
        this.keyGenerator = keyGenerator;
    }

    public TreeNode getRoot() {
        return root;
    }

    public ArrayList<LeafNode> getLeafs() {
        ArrayList<LeafNode> leafs = new ArrayList<>();
        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Queue<TreeNode> childrenQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                TreeNode cur = queue.poll();
                if (cur == null) {
                    continue;
                }
                if (cur.isLeaf()) {
                    leafs.add((LeafNode) cur);
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

    public void insert(STObject object) {
//        System.out.println(object);
        TreeNode cur = root;
        double lat = object.getLat();
        double lon = object.getLon();
        while (!cur.isLeaf())  {
            cur = cur.getChild(lat, lon);
        }
        if (cur.isLeaf()) {
            if (keyGenerator == null) {
                System.err.println("please set key generator!");
            } else {
                byte[] stKey = keyGenerator.toKey(object);
                int n = stKey.length;
                for (String s : object.getKeywords()) {
                    cur.add(ByteUtil.concat(Bytes.toBytes(s.hashCode()), Arrays.copyOf(stKey, n - 8)));
                    // System.out.println("x" + Arrays.toString(ByteUtil.concat(Bytes.toBytes(s.hashCode()), Arrays.copyOf(stKey, n - 8))));
                }
            }
        }
    }

    public ArrayList<Range<byte[]>> toFilteredKeyRanges(Query query) {
        ArrayList<Range<byte[]>> ranges = new ArrayList<>();

        Queue<TreeNode> queue = new LinkedList<>();
        MBR mbr = query.getMBR();

        if (!mbr.intersects(root.getMbr())) {
            return ranges;
        }

        queue.add(root);
        ArrayList<LeafNode> leafs = new ArrayList<>();

        while (!queue.isEmpty()) {
            TreeNode cur = queue.poll();
            if (cur.isLeaf()) {
                leafs.add((LeafNode) cur);
            } else {
                for (TreeNode child : cur.getChildren()) {
                    if (child == null) {
                        continue;
                    }
                    if (mbr.intersects(child.getMbr())) {
                        queue.add(child);
                    }
                }
            }
        }

        Set<byte[]> set = new HashSet<>();
        for (LeafNode leaf : leafs) {
            MBR queryMBR = mbr.getIntersection(leaf.getMbr());
            query.setMbr(queryMBR);
//            System.out.println(leaf.getBloomFilter().approximateElementCount());
//            System.out.println(leaf.getMbr());
            set.addAll(keyGenerator.toFilteredKeys(query, leaf.getBloomFilter()));
        }
//        System.out.println("set size: " + set.size());
        return keyGenerator.keysToRanges(set.stream());
    }

    public void print() {
        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(root);

        while (!queue.isEmpty()) {
            Queue<TreeNode> childrenQueue = new LinkedList<>();
            System.out.println("*****************");
            while (!queue.isEmpty()) {
                TreeNode cur = queue.poll();
                if (cur == null) {
                    continue;
                }
                if (cur.isLeaf()) {
                    System.out.println("------------");
                    System.out.println("leaf");
                    System.out.println(cur.getMbr());
                    System.out.println("------------");
                } else {
                    childrenQueue.addAll(Arrays.asList(cur.getChildren()));
                }
            }
            System.out.println("*****************");
            System.out.println("\n");
            while (!childrenQueue.isEmpty()) {
                queue.add(childrenQueue.poll());
            }
        }
    }
}
