package com.START.STKQ;

import com.START.STKQ.exp.*;
import com.START.STKQ.io.DataReader;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;
import java.text.ParseException;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, InterruptedException, ClassNotFoundException {
//        TestWriteBloomToTxt.main(args);
        TestQueryTweetAll.main(args);
//        TestWriteTweetAll.main(args);
//        TestBloomMemory.main(args);
//        TestWrite.main(args);
//        TestQuery.main(args);
    }
}
