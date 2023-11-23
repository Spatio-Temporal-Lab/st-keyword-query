package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;

import java.util.List;

public interface ISTKFilter {
    void insert(STObject object);
    List<byte[]> shrink(Query query);
    List<Range<byte[]>> shrinkAndMerge(Query query);
}
