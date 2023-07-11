package com.START.STKQ.keyGenerator;

import com.START.STKQ.model.Query;
import com.START.STKQ.model.Range;
import com.START.STKQ.util.ByteUtil;
import com.START.STKQ.util.DateUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class TimeKeyGenerator implements IKeyGenerator<Date>, Serializable {
    private final int BYTE_COUNT = 3;
    private final int hourPerBin;

    public TimeKeyGenerator() {
        hourPerBin = 1;
    }

    public TimeKeyGenerator(int hourPerBin) {
        this.hourPerBin = hourPerBin;
    }

    public int getByteCount() {
        return BYTE_COUNT;
    }

    @Override
    public byte[] toKey(Date d) {
        return ByteUtil.getKByte(getNumber(d), BYTE_COUNT);
    }

    public Integer getNumber(Date d) {
        return DateUtil.getHours(d);
    }

    public Date keyToTime(byte[] key) {
        //TODO
        return null;
    }

    @Override
    public ArrayList<Range<byte[]>> toKeyRanges(Query query) {
        int ds = getNumber(query.getS());
        int dt = getNumber(query.getT());

        ArrayList<Range<byte[]>> ranges = new ArrayList<>();
        ranges.add(new Range<>(
                ByteUtil.getKByte(ds / hourPerBin, BYTE_COUNT),
                ByteUtil.getKByte(dt / hourPerBin, BYTE_COUNT)
        ));

        return ranges;
    }
}
