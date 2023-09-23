package org.urbcomp.startdb.stkq.initialization;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.io.DataProcessor;
import org.urbcomp.startdb.stkq.model.STObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

public class GenerateData {
//    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSample.csv";
private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSampleBig.csv";

    public static void main(String[] args) throws ParseException, IOException {
        generateTweetSampleData();
    }

    private static void generateTweetSampleData() throws ParseException, IOException {
        DataProcessor processor = new DataProcessor();
//        processor.setLimit(10_0000);
        processor.setLimit(100_0000);
        ArrayList<STObject> objects = processor.getSTObjects(Constant.TWEET_DIR);

        try (BufferedWriter out = new BufferedWriter(new FileWriter(TWEET_SAMPLE_FILE))) {
            for (STObject object : objects) {
                out.write(object.toVSCLine() + '\n');
            }
        }
    }
}
