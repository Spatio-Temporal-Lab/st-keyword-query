package com.START.STKQ.io;

import com.START.STKQ.keyGenerator.AbstractSTKeyGenerator;
import com.START.STKQ.keyGenerator.SpatialKeyGenerator;
import com.START.STKQ.keyGenerator.TimeFirstSTKeyGenerator;
import com.START.STKQ.keyGenerator.TimeKeyGenerator;
import com.START.STKQ.model.STObject;
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
        DataReader dataReader = new DataReader();
        dataReader.setRate(0.01);
        ArrayList<STObject> objects = new ArrayList<>();
        for (int i = 1; i <= 1; ++i) {
            objects.addAll(dataReader.getSTObjects("E:\\data\\tweet\\" + "tweet_" + i + ".csv"));
        }
        hBaseWriter.putObjects(tableName, objects, 5000);
    }
}