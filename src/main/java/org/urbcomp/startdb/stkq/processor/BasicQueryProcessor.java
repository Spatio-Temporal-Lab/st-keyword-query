package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;
import java.util.Map;

public class BasicQueryProcessor extends AbstractQueryProcessor {
    private final ISTKeyGenerator keyGenerator;

    public BasicQueryProcessor(String tableName, ISTKeyGenerator keyGenerator) {
        super(tableName);
        this.keyGenerator = keyGenerator;
    }

    @Override
    public List<Range<byte[]>> getRanges(Query query) {
        return keyGenerator.toBytesRanges(query);
    }

    @Override
    protected List<Map<String, String>> HBaseScan(String tableName, List<Range<byte[]>> ranges, Query query) throws InterruptedException {
        return HBaseQueryProcessor.scan(tableName, ranges, query);
    }
}
