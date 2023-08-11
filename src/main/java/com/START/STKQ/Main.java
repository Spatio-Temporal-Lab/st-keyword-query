package com.START.STKQ;

import com.START.STKQ.model.BytesKey;
import com.START.STKQ.util.ByteUtil;
import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, InterruptedException, ClassNotFoundException {

//        ArrayList<String> keywords = new ArrayList<>();
//        keywords.add("1");
//        keywords.add("2");
//        keywords.add("3");
//        byte[][] wordsCode = keywords.stream().map(word -> ByteUtil.getKByte(word.hashCode(), 4)).toArray(byte[][]::new);
//        System.out.println(Arrays.deepToString(wordsCode));
//        ArrayList<byte[]> temp = new ArrayList<>();
//        for (String s : keywords) {
//            temp.add(ByteUtil.getKByte(s.hashCode(), 4));
//        }
//        for (byte[] bytes : temp) {
//            System.out.println(Arrays.toString(bytes));
//        }


//        ChainedInfiniFilter filter1 = new ChainedInfiniFilter(20, 10);
//        filter1.set_expand_autonomously(true);
//        System.out.println(RamUsageEstimator.humanSizeOf(filter1));
//
//        ChainedInfiniFilter filter2 = new ChainedInfiniFilter(3, 10);
//        filter2.set_expand_autonomously(true);
//        System.out.println(RamUsageEstimator.humanSizeOf(filter2));
//
//        ChainedInfiniFilter filter3 = new ChainedInfiniFilter(1, 10);
//        filter3.set_expand_autonomously(true);
//        System.out.println(RamUsageEstimator.humanSizeOf(filter3));
//
//        int n = 100;
//        for (int i = 0; i < n; ++i) {
//            for (int j = 0; j < n; ++j) {
//                for (int k = 0; k < n; ++k) {
//                    filter1.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
//                    filter2.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
//                    filter3.insert(new byte[]{(byte) i, (byte) j, (byte) k}, false);
//                }
//            }
//        }
//
//        int error1 = 0;
//        int error2 = 0;
//        int error3 = 0;
//        for (int i = 0; i < n; ++i) {
//            for (int j = 0; j < n; ++j) {
//                for (int k = 0; k < n; ++k) {
//                    if (!filter1.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
//                        ++error1;
//                    }
//                    if (!filter2.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
//                        ++error2;
//                    }
//                    if (!filter3.search(new byte[]{(byte) i, (byte) j, (byte) k})) {
//                        ++error3;
//                    }
//                }
//            }
//        }
//        System.out.println(error1 + " " + error2 + " " + error3);
//        System.out.println(RamUsageEstimator.humanSizeOf(filter1));
//        System.out.println(RamUsageEstimator.humanSizeOf(filter2));
//        System.out.println(RamUsageEstimator.humanSizeOf(filter3));
    }
}
