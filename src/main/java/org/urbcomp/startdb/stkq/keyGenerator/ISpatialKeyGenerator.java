package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Location;

public interface ISpatialKeyGenerator extends IKeyGenerator<Location, Long>, IKeyRangeGenerator<Long> {
    public int getBits();
}
