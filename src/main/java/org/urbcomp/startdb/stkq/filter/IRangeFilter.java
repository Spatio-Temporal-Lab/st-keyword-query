package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.STObject;

import java.util.List;

public interface IRangeFilter {
    void insert(STObject object);
    List<byte[]> shrink(Query query);
}
