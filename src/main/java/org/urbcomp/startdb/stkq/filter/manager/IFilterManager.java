package org.urbcomp.startdb.stkq.filter.manager;

import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

public interface IFilterManager {
    IFilter getAndCreateIfNoExists(BytesKey index);

    IFilter get(BytesKey index);

    IFilter getWithIO(BytesKey index);

    IFilter update(BytesKey index);

    IFilter getAndUpdate(BytesKey index);
}
