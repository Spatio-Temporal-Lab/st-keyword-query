package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;

import java.io.IOException;
import java.util.List;

public interface ISTKFilter {
    void insert(STObject object) throws IOException;
    List<byte[]> shrink(Query query) throws IOException;
    List<Range<byte[]>> shrinkAndMerge(Query query) throws IOException;
}
