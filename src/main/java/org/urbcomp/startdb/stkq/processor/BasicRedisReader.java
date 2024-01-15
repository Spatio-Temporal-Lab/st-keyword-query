package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class BasicRedisReader implements Closeable {
    private final STKeyGenerator keyGenerator;

    public BasicRedisReader(STKeyGenerator generator) {
        this.keyGenerator = generator;
    }

    public List<Range<Long>> getRanges(Query query) {
        return keyGenerator.toNumberRanges(query);
    }

    public List<STObject> scan(Query query) throws IOException {
        return RedisQueryProcessor.scan(getRanges(query), query);
    }

    @Override
    public void close() throws IOException {
        RedisIO.close();
    }
}
