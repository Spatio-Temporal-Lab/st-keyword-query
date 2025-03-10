package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.IOException;

public interface IFilterManager {
    IFilter getAndCreateIfNoExists(BytesKey index, boolean readFromDb) throws IOException;

    IFilter get(BytesKey index) throws IOException;

    IFilter getWithIO(BytesKey index);

    void out();

    long ramUsage();
}
