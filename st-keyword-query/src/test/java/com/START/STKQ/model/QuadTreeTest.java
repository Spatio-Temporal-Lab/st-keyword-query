package com.START.STKQ.model;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.io.DataReader;
import com.START.STKQ.keyGenerator.SpatialFirstSTKeyGenerator;
import com.START.STKQ.util.DateUtil;
import junit.framework.TestCase;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

public class QuadTreeTest extends TestCase {
    public void testBuild() throws ParseException, IOException {
        int n = 200_0000;

        DataReader dataReader = new DataReader();
        dataReader.setLimit(n);
        ArrayList<STObject> objects = dataReader.getSTObjects("E:\\data\\tweet\\tweetAll.csv");

//        ArrayList<STObject> objects = new ArrayList<>();
//        Date date1 = DateUtil.getDate("2000-01-01 11:00:00");
//        Date date = DateUtil.getDate("2000-01-01 12:00:00");
//        Date date2 = DateUtil.getDate("2000-01-01 13:00:00");
//        String s = "a";
//        ArrayList<String> ss = new ArrayList<>();
//        ss.add(s);
//        objects.add(new STObject(0, -10, -10, date, ss));
//        objects.add(new STObject(1, 10, -10, date, ss));
//        objects.add(new STObject(2, -10, 10, date, ss));
//        objects.add(new STObject(3, 10, 10, date, ss));
//        objects.add(new STObject(4, -10, -10, date1, ss));
//        objects.add(new STObject(5, -10, -10, date2, ss));

        QuadTree tree = new QuadTree(14, 100);
        tree.build(objects);

        STQuadTree tree1 = new STQuadTree(tree.getRoot());
        tree1.setKeyGenerator(new SpatialFirstSTKeyGenerator());


//        for (STObject object : objects) {
//            tree1.insert(object);
//        }
////        tree1.print();
        String path = "E:\\data\\blooms\\tree.txt";
        try(FileOutputStream f = new FileOutputStream(path);
            ObjectOutputStream os = new ObjectOutputStream(f)) {
            os.writeObject(tree1);
        }
//
//        Query query = new Query(-10.01, -9.99, -10.01, -9.99, date1, date2, ss);
//        ArrayList<String> ss = new ArrayList<>();
//        ss.add("long");
//        Query query = new Query(35.95042563, 35.95042563, -83.9331553, -83.9331553, DateUtil.getDate("2012-08-28 05:30:50"),
//                DateUtil.getDate("2012-08-28 05:30:50"), ss);
//
//        query.setQueryType(QueryType.CONTAIN_ONE);
//        tree1.toFilteredKeyRanges(query);

//        STQuadTree tree2;
//        try(FileInputStream f = new FileInputStream(path);
//            ObjectInputStream is = new ObjectInputStream(f)) {
//            tree2 = (STQuadTree) is.readObject();
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
//        tree2.toFilteredKeyRanges(query);

    }
}