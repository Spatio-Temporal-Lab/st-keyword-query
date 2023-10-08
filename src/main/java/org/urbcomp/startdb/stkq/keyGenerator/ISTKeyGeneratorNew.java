package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.util.List;

public interface ISTKeyGeneratorNew extends IKeyGeneratorNew<STObject, Long>, IKeyRangeGeneratorNew<Long>{
    default byte[] toDatabaseKey(STObject stObject) {
        return ByteUtil.concat(toBytes(stObject), ByteUtil.longToByte(stObject.getID()));
    }

    List<Range<byte[]>> toBytesRanges(Query query);

    int getByteCount();
}
