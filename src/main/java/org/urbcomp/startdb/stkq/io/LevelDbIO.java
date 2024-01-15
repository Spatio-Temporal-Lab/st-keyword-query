package org.urbcomp.startdb.stkq.io;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.urbcomp.startdb.stkq.filter.IFilter;
import org.urbcomp.startdb.stkq.filter.InfiniFilter;
import org.urbcomp.startdb.stkq.model.BytesKey;

import java.io.*;
import java.util.Map;
import java.util.Objects;

public class LevelDbIO implements Closeable {
    private static DB db = null;
    private static final String dbFolder ="D:\\data\\levelDB";

    static {
        DBFactory factory = new Iq80DBFactory();
        Options options = new Options();
        options.createIfMissing(true);
        try {
            db = factory.open(new File(dbFolder), options);
        } catch (IOException e) {
            System.out.println("levelDB启动异常");
            e.printStackTrace();
        }
    }

    public static void putFilters(Map<BytesKey, IFilter> filters) throws IOException {
        try(WriteBatch batch = db.createWriteBatch()) {
            for (Map.Entry<BytesKey, IFilter> entry : filters.entrySet()) {
                byte[] key = entry.getKey().getArray();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                entry.getValue().writeTo(bos);
                byte[] value = bos.toByteArray();
                batch.put(key, value);
            }
            db.write(batch);
        }
    }

    public static IFilter getFilter(byte[] key) {
        byte[] values = db.get(key);
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return new InfiniFilter(temp.read(bis));
    }

    public static ChainedInfiniFilter getFilterInChainType(byte[] key) {
        byte[] values = db.get(key);
        if (values == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(values);
        ChainedInfiniFilter temp = new ChainedInfiniFilter();
        return temp.read(bis);
    }

    public static void putFilter(byte[] key, IFilter filter) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        filter.writeTo(bos);
        byte[] value = bos.toByteArray();
        db.put(key, value);
    }

    public static void clearData() {
        clearFolder(new File(dbFolder));
    }

    public static void clearFolder(File folder) {
        if (folder.isDirectory()) {
            for (File file : Objects.requireNonNull(folder.listFiles())) {
                clearFolder(file);
            }
        }
        folder.delete();
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    public static void main(String[] args) throws IOException {
        clearData();
    }
}
