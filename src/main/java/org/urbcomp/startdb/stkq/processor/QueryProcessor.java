package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.ISTKFilter;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class QueryProcessor extends AbstractQueryProcessor {
    private final ISTKFilter filter;

    public QueryProcessor(String tableName, ISTKFilter filter) {
        super(tableName);
        this.filter = filter;
    }

    @Override
    public List<Range<byte[]>> getRanges(Query query) throws IOException {
        return filter.shrinkAndMerge(query);
    }

    @Override
    protected List<Map<String, String>> HBaseScan(String tableName, List<Range<byte[]>> ranges, Query query) throws InterruptedException {
        return HBaseQueryProcessor.scan(tableName, ranges, query);
    }
}
