package org.urbcomp.startdb.stkq.constant;

public class Constant {

    public static final int TIME_BYTE_COUNT = 3;
    public static final int SPATIAL_BYTE_COUNT = 4;

    public static final int KEYWORD_BYTE_COUNT = 4;

    public static String DATA_DIR = System.getProperty("os.name").startsWith("Win") ? "E:\\data\\" : "/usr/data/";
    public static String TWEET_DIR = DATA_DIR + "tweetAll.csv";

}
