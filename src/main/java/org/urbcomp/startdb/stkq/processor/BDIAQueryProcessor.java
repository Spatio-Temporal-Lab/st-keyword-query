package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;
import java.util.Map;

// Chen X, Zhang C, Shi Z, et al. Spatio-temporal keywords queries in HBase[J]. Big Data & Information Analytics, 2015, 1(1): 81-91.
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
    protected List<Map<String, String>> HBaseScan(String tableName, List<Range<byte[]>> ranges, Query query) throws InterruptedException {
        return HBaseQueryProcessor.scanBDIA(tableName, ranges, query);
    }
}
