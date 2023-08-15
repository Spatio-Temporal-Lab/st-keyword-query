package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.constant.Constant;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class TimeKeyGeneratorNew implements IKeyGeneratorNew<Date, Integer>, IKeyRangeGeneratorNew<Integer> {
    private final static int BYTE_COUNT = Constant.TIME_BYTE_COUNT;
    private final static int DEFAULT_HOURS_PER_BIN = 1;
    private final int hoursPerBin;

    public TimeKeyGeneratorNew() {
        this(DEFAULT_HOURS_PER_BIN);
    }

    public TimeKeyGeneratorNew(int hoursPerBin) {
        this.hoursPerBin = hoursPerBin;
    }

    @Override
    public Integer toNumber(Date object) {
        return DateUtil.getHours(object) / hoursPerBin;
    }

    @Override
    public byte[] toBytes(Date object) {
        return ByteUtil.getKByte(toNumber(object), BYTE_COUNT);
    }

    @Override
    public List<Range<Integer>> toNumberRanges(Query query) {
        int ds = toNumber(query.getStartTime());
        int dt = toNumber(query.getEndTime());
        return Collections.singletonList(new Range<>(ds / hoursPerBin, dt / hoursPerBin));
    }

    @Override
    public List<Range<byte[]>> toBytesRanges(Query query) {
        return toNumberRanges(query).stream()
                .map(o -> new Range<>(ByteUtil.getKByte(o.getLow(), BYTE_COUNT), ByteUtil.getKByte(o.getHigh(), BYTE_COUNT)))
                .collect(Collectors.toList());
    }
}
