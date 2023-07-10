package com.START.STKQ.model;

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
        for (byte b : array) {
            builder.append(b);
        }
        return builder.toString();
    }
}

