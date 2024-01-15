package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.filter.ISTKFilter;
import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class RedisReader implements Closeable {
    private final ISTKFilter filter;

    public RedisReader(ISTKFilter filter) {
        this.filter = filter;
    }

    public List<Range<Long>> getRanges(Query query) throws IOException {
        return filter.shrinkAndMergeLong(query);
    }

    public List<STObject> scan(Query query) throws IOException {
        return RedisQueryProcessor.scan(getRanges(query), query);
    }

    @Override
    public void close() throws IOException {
        RedisIO.close();
    }
}
