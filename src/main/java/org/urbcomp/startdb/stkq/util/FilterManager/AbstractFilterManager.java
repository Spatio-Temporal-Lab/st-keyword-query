package org.urbcomp.startdb.stkq.util.FilterManager;

import org.urbcomp.startdb.stkq.model.BytesKey;
import com.github.nivdayan.FilterLibrary.filters.Filter;

public abstract class AbstractFilterManager {
    public static Filter getFilter(BytesKey bytesKey) {
        return null;
    }

    public static void reAllocate() {
    }

    public static void init() {

    }
}
