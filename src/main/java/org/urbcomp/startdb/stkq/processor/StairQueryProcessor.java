package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.AbstractSTFilter;
import org.urbcomp.startdb.stkq.filter.StairBF;
import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;

public class StairQueryProcessor extends BasicQueryProcessor {
    private StairBF filter;

    public StairQueryProcessor(String tableName, ISTKeyGeneratorNew keyGenerator) {
        super(tableName, keyGenerator);
    }

    public StairQueryProcessor(String tableName, StairBF stFilter) {
        super(tableName, null);
        this.filter = stFilter;
        filterInMemory = true;
    }

    public StairQueryProcessor(String tableName, ISTKeyGeneratorNew keyGenerator, StairBF stFilter) {
        super(tableName, keyGenerator);
        this.filter = stFilter;
        filterInMemory = true;
    }

    @Override
    public List<Range<byte[]>> shrink(Query query) {
        return filter.shrinkAndTransform(query);
    }

    public void close() {
        HBaseQueryProcessor.close();
    }
}
