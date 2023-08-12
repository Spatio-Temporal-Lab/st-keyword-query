package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.constant.QueryType;
import com.google.common.hash.BloomFilter;

import java.util.ArrayList;

public class BfUtil {
    public static boolean check(BloomFilter<byte[]> bf, byte[] key, ArrayList<String> keywords, QueryType queryType) {
        ArrayList<byte[]> keyPres = new ArrayList<>();
        for (String s : keywords) {
            keyPres.add(ByteUtil.intToByte(s.hashCode()));
        }
        switch (queryType) {
            case CONTAIN_ONE:
                for (byte[] keyPre : keyPres) {
                    if (bf.mightContain(ByteUtil.concat(keyPre, key))) {
                        return true;
                    }
                }
                return false;
            case CONTAIN_ALL:
                for (byte[] keyPre : keyPres) {
                    if (!bf.mightContain(ByteUtil.concat(keyPre, key))) {
                        return false;
                    }
                }
                return true;
        }
        return true;
    }
}
