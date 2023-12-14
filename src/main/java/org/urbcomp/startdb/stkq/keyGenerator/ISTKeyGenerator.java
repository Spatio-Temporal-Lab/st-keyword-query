package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.List;

public interface ISTKeyGenerator extends IKeyGenerator<STObject, Long>, IKeyRangeGenerator<Long> {
    default byte[] toDatabaseKey(STObject stObject) {
//        return ByteUtil.concat(toBytes(stObject), ByteUtil.longToBytes(stObject.getID()));
        return ByteUtil.concat(toBytes(stObject), ByteUtil.longToBytesWithoutPrefixZero(stObject.getID()));
    }

    List<Range<byte[]>> toBytesRanges(Query query);

    int getByteCount();
}
