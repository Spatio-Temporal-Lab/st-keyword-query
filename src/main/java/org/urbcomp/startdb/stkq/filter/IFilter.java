package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.constant.QueryType;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.ArrayList;

public interface IFilter {
    ArrayList<byte[]> filter(ArrayList<Range<Long>> sRanges, Range<Integer> tRange, ArrayList<String> keywords, QueryType queryType);
    void insert(byte[] code);
}
