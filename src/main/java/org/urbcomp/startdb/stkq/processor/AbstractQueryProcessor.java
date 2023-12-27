package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public abstract class AbstractQueryProcessor implements Closeable {
    protected final String tableName;

    public AbstractQueryProcessor(String tableName) {
        this.tableName = tableName;
    }

    protected abstract List<Range<byte[]>> getRanges(Query query) throws IOException;

    public List<STObject> getResult(Query query) throws InterruptedException, ParseException, IOException {
        List<Range<byte[]>> ranges = getRanges(query);

        List<Map<String, String>> scanResults = HBaseScan(tableName, ranges, query);

        List<STObject> result = new ArrayList<>();
        for (Map<String, String> map : scanResults) {
            Location loc = new Location(map.get("loc"));
            Date date = DateUtil.getDate(map.get("time"));
            List<String> keywords = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));
            result.add(new STObject(Long.parseLong(map.get("id")), loc.getLat(), loc.getLon(), date, keywords));
        }

        return result;
    }

    protected abstract List<Map<String, String>> HBaseScan(String tableName, List<Range<byte[]>> ranges,
                                                  Query query) throws InterruptedException;

    public void close() {
        HBaseQueryProcessor.close();
    }
}
