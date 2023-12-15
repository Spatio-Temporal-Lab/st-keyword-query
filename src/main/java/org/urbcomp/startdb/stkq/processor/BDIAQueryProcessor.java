package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.text.ParseException;
import java.util.*;

public class BDIAQueryProcessor extends AbstractQueryProcessor {
    private final ISTKeyGenerator keyGenerator;

    public BDIAQueryProcessor(String tableName, ISTKeyGenerator keyGenerator) {
        super(tableName);
        this.keyGenerator = keyGenerator;
    }

    public List<Range<byte[]>> getRanges(Query query) {
        return keyGenerator.toBytesRanges(query);
    }

    @Override
    public ArrayList<STObject> getResult(Query query) throws InterruptedException, ParseException {
        List<Map<String, String>> scanResults;

        List<Range<byte[]>> ranges;

        long begin = System.currentTimeMillis();
        ranges = getRanges(query);
        long end = System.currentTimeMillis();
        rangeGenerateTime += end - begin;

        allSize += ranges.size();
        allCount += getRangesSize(ranges);

        begin = System.currentTimeMillis();
        scanResults = HBaseQueryProcessor.scanBDIA(tableName, ranges, query);
        end = System.currentTimeMillis();
        queryDbTime += end - begin;

        ArrayList<STObject> result = new ArrayList<>();
        for (Map<String, String> map : scanResults) {
            Location loc = new Location(map.get("loc"));
            Date date = DateUtil.getDate(map.get("time"));
            ArrayList<String> keywords = new ArrayList<>(Arrays.asList(map.get("keywords").split(" ")));
            result.add(new STObject(Long.parseLong(map.get("id")), loc.getLat(), loc.getLon(), date, keywords));
        }

        return result;
    }

}
