package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TimeKeyGenerator implements IKeyGenerator<Date>, Serializable {
    private final int BYTE_COUNT = Constant.TIME_BYTE_COUNT;
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
        return DateUtil.getHours(d) / hourPerBin;
    }

    @Override
    public List<Range<byte[]>> toKeyRanges(Query query) {
        int ds = getNumber(query.getStartTime());
        int dt = getNumber(query.getEndTime());

        List<Range<byte[]>> ranges = new ArrayList<>();
        ranges.add(new Range<>(
                ByteUtil.getKByte(ds / hourPerBin, BYTE_COUNT),
                ByteUtil.getKByte(dt / hourPerBin, BYTE_COUNT)
        ));

        return ranges;
    }

    public Range<Integer> toRanges(Query query) {
        int ds = getNumber(query.getStartTime());
        int dt = getNumber(query.getEndTime());

        return new Range<>(ds / hourPerBin, dt / hourPerBin);
    }
}
