package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

public interface ISTKeyGeneratorNew extends IKeyGeneratorNew<STObject, Long>, IKeyRangeGeneratorNew<Long>{
    default byte[] toDatabaseKey(STObject stObject) {
        return ByteUtil.concat(toBytes(stObject), ByteUtil.longToByte(stObject.getID()));
    }
}
