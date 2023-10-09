package org.urbcomp.startdb.stkq.io;

import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.model.STObject;
import junit.framework.TestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

public class HBaseWriterTest extends TestCase {

    public void testPutObjects() throws IOException, ParseException {
        HBaseUtil hBaseUtil = HBaseUtil.getDefaultHBaseUtil();
        String tableName = "test01";
        hBaseUtil.createTable(tableName, new String[]{"attr"});
//        AbstractSTKeyGenerator keyGenerator = new TimeFirstSTKeyGenerator(new SpatialKeyGenerator(), new TimeKeyGenerator());
        ISTKeyGeneratorNew keyGenerator = new STKeyGenerator();
        HBaseWriter hBaseWriter = new HBaseWriter();
        DataProcessor dataProcessor = new DataProcessor();
        dataProcessor.setRate(0.01);
        ArrayList<STObject> objects = new ArrayList<>();
        for (int i = 1; i <= 1; ++i) {
            objects.addAll(dataProcessor.getSTObjects("E:\\data\\tweet\\" + "tweet_" + i + ".csv"));
        }
        hBaseWriter.putObjects(tableName, keyGenerator, objects, 5000);
    }
}
