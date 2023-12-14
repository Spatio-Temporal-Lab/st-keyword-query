package org.urbcomp.startdb.stkq.keyGenerator;

public interface IKeyGenerator<T, R> {
    R toNumber(T object);
    byte[] toBytes(T object);
    byte[] numberToBytes(R number);
}
