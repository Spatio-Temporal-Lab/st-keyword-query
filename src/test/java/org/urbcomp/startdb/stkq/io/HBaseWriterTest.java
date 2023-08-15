package org.urbcomp.startdb.stkq.io;

import org.urbcomp.startdb.stkq.keyGenerator.old.AbstractSTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.SpatialKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.TimeFirstSTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.old.TimeKeyGenerator;
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
        AbstractSTKeyGenerator keyGenerator = new TimeFirstSTKeyGenerator(new SpatialKeyGenerator(), new TimeKeyGenerator());
        HBaseWriter hBaseWriter = new HBaseWriter(keyGenerator);
        DataProcessor dataProcessor = new DataProcessor();
        dataProcessor.setRate(0.01);
        ArrayList<STObject> objects = new ArrayList<>();
        for (int i = 1; i <= 1; ++i) {
            objects.addAll(dataProcessor.getSTObjects("E:\\data\\tweet\\" + "tweet_" + i + ".csv"));
        }
        hBaseWriter.putObjects(tableName, objects, 5000);
    }
}
