package com.START.STKQ;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.text.ParseException;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, InterruptedException, ClassNotFoundException {
//        FilterManager.loadCount();
//        ChainedInfiniFilter filter = (ChainedInfiniFilter) FilterManager.getFilter(new BytesKey(new byte[]{13, -2, 1, -78}));
//        System.out.println(RamUsageEstimator.humanSizeOf(filter));
//        assert filter != null;
//        //[13, -2, 5, -95, 1, -78, 3]
//        byte[] keypre1 = new byte[]{-30, -108, -48, 46};
//        byte[] keypre2 = new byte[]{-82, 121, -61, 37};
//        long s = ByteUtil.toLong(new byte[]{13, -2, 5, -95});
//        byte[] ss = ByteUtil.getKByte(s >>> 4, 4);
//        int t = ByteUtil.toInt(new byte[]{1, -78, 3});
//        byte[] tt = ByteUtil.getKByte(t >>> 2, 3);
//        System.out.println(filter.search(ByteUtil.concat(keypre1, ss, tt)));
//        System.out.println(filter.search(ByteUtil.concat(keypre2, ss, tt)));
//        System.out.println(filter.search(new byte[]{-81, 67, -120, -24, 0, -33, -26, 84, 0, 108, -65}));

        ChainedInfiniFilter filter1 = new ChainedInfiniFilter(20, 10);
        filter1.set_expand_autonomously(true);
        System.out.println(RamUsageEstimator.humanSizeOf(filter1));

        ChainedInfiniFilter filter2 = new ChainedInfiniFilter(3, 10);
        filter2.set_expand_autonomously(true);
        System.out.println(RamUsageEstimator.humanSizeOf(filter2));

        ChainedInfiniFilter filter3 = new ChainedInfiniFilter(1, 10);
        filter3.set_expand_autonomously(true);
        System.out.println(RamUsageEstimator.humanSizeOf(filter3));

        int n = 100;
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                for (int k = 0; k < n; ++k) {
                    filter1.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
                    filter2.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
                    filter3.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
                }
            }
        }

        int error1 = 0;
        int error2 = 0;
        int error3 = 0;
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                for (int k = 0; k < n; ++k) {
                    if (!filter1.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
                        ++error1;
                    }
                    if (!filter2.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
                        ++error2;
                    }
                    if (!filter3.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
                        ++error3;
                    }
                }
            }
        }
        System.out.println(error1 + " " + error2 + " " + error3);
        System.out.println(RamUsageEstimator.humanSizeOf(filter1));
        System.out.println(RamUsageEstimator.humanSizeOf(filter2));
        System.out.println(RamUsageEstimator.humanSizeOf(filter3));


//        TestWriteBloomToTxt.main(args);
//        TestQueryTweet.main(args);
//        TestWriteTweetAll.main(args);
//        TestBloomMemory.main(args);
//        TestWrite.main(args);
//        TestQuery.main(args);
    }
}
