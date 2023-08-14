package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;

public interface IFilter {
    List<byte[]> filter(List<Range<Long>> sRanges, Range<Integer> tRange, List<String> keywords, QueryType queryType);
    void insert(byte[] code);
}
