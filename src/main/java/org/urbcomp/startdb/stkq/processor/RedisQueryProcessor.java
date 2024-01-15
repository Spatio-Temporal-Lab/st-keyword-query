package org.urbcomp.startdb.stkq.processor;

import org.urbcomp.startdb.stkq.io.RedisIO;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.STKUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisQueryProcessor {
    public static List<STObject> scan(List<Range<Long>> ranges, Query query) {
        if (ranges.isEmpty()) {
            return new ArrayList<>();
        }
        return RedisIO.rangeQueries(0, new byte[]{0}, ranges).stream().map(STObject::new)
                .filter(o -> STKUtil.check(o, query)).collect(Collectors.toCollection(ArrayList::new));
    }

    public static void close() {
        RedisIO.close();
    }

}
