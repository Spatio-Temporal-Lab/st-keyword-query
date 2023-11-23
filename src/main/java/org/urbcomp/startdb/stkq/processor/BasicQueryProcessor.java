package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.io.HBaseQueryProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;

public class BasicQueryProcessor extends AbstractQueryProcessor {
    private ISTKeyGeneratorNew keyGenerator;

    public BasicQueryProcessor(String tableName, ISTKeyGeneratorNew keyGenerator) {
        super(tableName);
        this.keyGenerator = keyGenerator;
    }

    @Override
    public List<Range<byte[]>> getRanges(Query query) {
        return keyGenerator.toBytesRanges(query);
    }

    public void close() {
        HBaseQueryProcessor.close();
    }
}
