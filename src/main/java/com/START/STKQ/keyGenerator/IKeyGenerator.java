package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;

import java.util.ArrayList;

public interface IKeyGenerator<T> {
    byte[] toKey(T object);

    ArrayList<Range<byte[]>> toKeyRanges(Query query);
}