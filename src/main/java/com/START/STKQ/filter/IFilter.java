package com.START.STKQ.filter;

import com.START.STKQ.constant.QueryType;
import com.START.STKQ.model.Range;

import java.util.ArrayList;

public interface IFilter {
    ArrayList<byte[]> filter(ArrayList<Range<Long>> sRanges, Range<Integer> tRange, ArrayList<String> keywords, QueryType queryType);
    void insert(byte[] code);
}
