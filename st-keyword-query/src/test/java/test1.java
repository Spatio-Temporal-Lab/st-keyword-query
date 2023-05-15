import com.START.STKQ.io.DataReader;
import com.START.STKQ.model.STQuadTree;
import com.START.STKQ.util.DateUtil;
import com.github.StairSketch.StairBf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.ParseException;


public class test1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException {

        DataReader dataReader = new DataReader();
//        dataReader.setLimit(100);
        StairBf bf = dataReader.generateStairBloomFilters("E:\\data\\tweet\\tweetAll.csv", 4, 4,0.01);
        FileOutputStream f = new FileOutputStream("E:\\data\\blooms\\stair.txt");
        ObjectOutputStream o = new ObjectOutputStream(f);
        o.writeObject(bf);
//        STQuadTree tree = dataReader.generateTree("E:\\data\\tweet\\tweetAll.csv");
//
//        System.out.println(DateUtil.getHours(
//                DateUtil.getDate("2012-04-01 00:10:00"),
//                DateUtil.getDate("2012-12-28 07:14:07")
//        ));


//        FileInputStream fi = new FileInputStream("E:\\data\\blooms\\treeFull.txt");
//        ObjectInputStream oi = new ObjectInputStream(fi);
//        STQuadTree tree = (STQuadTree) oi.readObject();
//        ArrayList<LeafNode> leafNodes = tree.getLeafs();
//        for (LeafNode node : leafNodes) {
//            System.out.println(node.getBloomFilter().expectedFpp());
//        }

//        dataReader.getSTObjectsBySpatial("E:\\data\\tweet\\tweetAll.csv");

//        int size = 103991699;
//        int size = 2174348;
//        int bitCount = (int) (-size * Math.log(0.001) / Math.log(2) / Math.log(2));
//        System.out.println(bitCount);
//        System.out.println(bitCount / 8 / 1024 / 1024 + " MB");
//        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), size, 0.001);

//        FileOutputStream f = new FileOutputStream(new File("E:\\data\\myObjects.txt"));
//        ObjectOutputStream o = new ObjectOutputStream(f);
//        BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), 1000, 0.001);
//
//        for (int i = 0; i < 1000; ++i) {
//            bloomFilter.put(ByteUtil.intToByte(i));
//        }
//        for (int i = 0; i < 1000; ++i) {
//            if (!bloomFilter.mightContain(ByteUtil.intToByte(i))) {
//                System.out.println(i);
//            }
//        }
//
//        o.writeObject(bloomFilter);
//
//        FileInputStream fi = new FileInputStream(new File("E:\\data\\myObjects.txt"));
//        ObjectInputStream oi = new ObjectInputStream(fi);
//
//        BloomFilter<byte[]> bloomFilter1 = (BloomFilter<byte[]>) oi.readObject();
//
//        for (int i = 0; i < 1000; ++i) {
//            if (!bloomFilter.mightContain(ByteUtil.intToByte(i))) {
//                System.out.println("xxx " + i);
//            }
//        }
//
//        System.out.println(RamUsageEstimator.humanSizeOf(bloomFilter));
    }
}