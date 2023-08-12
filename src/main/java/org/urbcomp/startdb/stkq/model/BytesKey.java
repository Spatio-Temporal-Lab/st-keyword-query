package org.urbcomp.startdb.stkq.model;

import java.io.Serializable;
import java.util.Arrays;

public class BytesKey implements Serializable {
    private final byte[] array;

    public BytesKey(byte[] array) {
        this.array = array;
    }

    public byte[] getArray() {
        return array.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        BytesKey bytesKey = (BytesKey) o;
        return Arrays.equals(array, bytesKey.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        int n = array.length;
        for (int i = 0; i < n - 1; ++i) {
            builder.append(array[i]).append("_");
        }
        builder.append(array[array.length - 1]);
        return builder.toString();
    }
}

