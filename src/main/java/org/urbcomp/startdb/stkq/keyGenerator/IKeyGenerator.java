package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.ArrayList;

public interface IKeyGenerator<T> {
    byte[] toKey(T object);

    ArrayList<Range<byte[]>> toKeyRanges(Query query);
}
