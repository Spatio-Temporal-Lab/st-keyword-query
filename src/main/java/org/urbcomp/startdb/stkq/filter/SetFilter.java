package org.urbcomp.startdb.stkq.filter;

import org.urbcomp.startdb.stkq.model.BytesKey;

import java.util.HashSet;
import java.util.Set;

/**
 * use the set to verify if the element is contained in a set, which achieve zero false positive rate
 */
public class SetFilter implements IFilter {

    private final Set<BytesKey> set = new HashSet<>();

    @Override
    public boolean check(byte[] key) {
        return set.contains(new BytesKey(key));
    }

    @Override
    public boolean sacrifice() {
        return false;
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public void insert(byte[] code) {
        set.add(new BytesKey(code));
    }
}
