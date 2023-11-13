package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.AbstractSTFilter;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;

public class QueryProcessor extends BasicQueryProcessor {
    private AbstractSTFilter stFilter;

    public QueryProcessor(String tableName, ISTKeyGeneratorNew keyGenerator) {
        super(tableName, keyGenerator);
    }

    public QueryProcessor(String tableName,  AbstractSTFilter stFilter) {
        super(tableName, null);
        this.stFilter = stFilter;
        filterInMemory = true;
    }

    public QueryProcessor(String tableName, ISTKeyGeneratorNew keyGenerator, AbstractSTFilter stFilter) {
        super(tableName, keyGenerator);
        this.stFilter = stFilter;
        filterInMemory = true;
    }

    @Override
    public List<Range<byte[]>> shrink(Query query) {
        return stFilter.shrinkWithIOAndTransform(query);
    }

    public void close() {
        HBaseQueryProcessor.close();
    }
}
