package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.util.ByteUtil;

public class KeywordKeyGeneratorNew implements IKeyGeneratorNew<String, Integer>{
    @Override
    public Integer toNumber(String object) {
        return object.hashCode();
    }

    @Override
    public byte[] toBytes(String object) {
        return ByteUtil.getKByte(toNumber(object), Constant.KEYWORD_BYTE_COUNT);
    }
}
