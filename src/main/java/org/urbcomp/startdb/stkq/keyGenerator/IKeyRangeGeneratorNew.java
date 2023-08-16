package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;

import java.util.List;

public interface IKeyRangeGeneratorNew<T> {
    List<Range<T>> toNumberRanges(Query query);
}
