package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.HashSet;
import java.util.Set;

public class SetFilter extends BaseFilter {

    private final Set<BytesKey> set = new HashSet<>();

    @Override
    public boolean check(byte[] key) {
        return set.contains(new BytesKey(key));
    }

    @Override
    public void insert(byte[] code) {
        set.add(new BytesKey(code));
    }
}
