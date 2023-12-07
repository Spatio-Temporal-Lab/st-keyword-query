package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.ISTKFilter;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.io.IOException;
import java.util.List;

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

    public void close() {
        HBaseQueryProcessor.close();
    }
}
