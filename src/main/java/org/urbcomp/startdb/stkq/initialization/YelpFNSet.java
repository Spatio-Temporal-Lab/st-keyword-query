package org.urbcomp.startdb.stkq.initialization;

import org.urbcomp.startdb.stkq.model.BytesKey;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class YelpFNSet {
    static Set<BytesKey> keys = new HashSet<>();
    public static void insert(BytesKey key) {
        keys.add(key);
    }

    public static boolean check(BytesKey key) {
        return keys.contains(key);
    }

    public static void init() throws IOException {
        String path = new File("").getAbsolutePath() + "/src/main/resources/" + "yelpFNSet";
        System.out.println(path);

        File file = new File(path);

        try(ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(file.toPath()));) {
            keys = (Set<BytesKey>) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void output() throws IOException {
        String path = new File("").getAbsolutePath() + "/src/main/resources/" + "yelpFNSet";
        System.out.println(path);

        File file = new File(path);

        try(ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(file.toPath()));) {
            oos.writeObject(keys);
        }
    }

    //just for test
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 100; ++i) {
            insert(new BytesKey(ByteUtil.getKByte(i, 4)));
        }
        output();
        init();
        for (int i = 0; i < 100; ++i) {
            if (!check(new BytesKey(ByteUtil.getKByte(i, 4)))) {
                System.err.println("error for " + i);
            }
        }
    }
}
