package org.urbcomp.startdb.stkq.keyGenerator;

public interface IKeyGeneratorNew<T, R> {
    R toNumber(T object);
    byte[] toBytes(T object);
}
