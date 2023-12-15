package org.urbcomp.startdb.stkq.keyGenerator;

import org.junit.Assert;
import org.junit.Test;
import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertNotEquals;

public class SpatialKeyGeneratorTest {

    @Test
    public void testToKey() {
        ISpatialKeyGenerator generator = new Z2SpatialKeyGenerator();
        byte[] bytes1 = generator.toBytes(new Location(0, 0));
        assertNotEquals(ByteUtil.getValue(bytes1), BigInteger.valueOf(0));
        byte[] bytes2 = generator.toBytes(new Location(-90, -180));
        Assert.assertEquals(ByteUtil.getValue(bytes2), BigInteger.valueOf(0));
        byte[] bytes3 = generator.toBytes(new Location(90, 180));
        Assert.assertEquals(ByteUtil.getValue(bytes3), BigInteger.valueOf((1 << 28) - 1));
    }

    @Test
    public void testToKeyRanges() {
        ISpatialKeyGenerator[] generators = {
                new Z2SpatialKeyGenerator(),
                new HilbertSpatialKeyGenerator()
        };

        Random rand = new Random();
        for (ISpatialKeyGenerator generator : generators) {
            for (int i = 0; i < 100; ++i) {
                double x = rand.nextDouble() * 180 - 90;
                double y = rand.nextDouble() * 360 - 180;
                Location loc = new Location(x, y);

                long key = generator.toNumber(loc);
                Query query = new Query(x - 0.001, x + 0.001, y - 0.001, y + 0.001,
                        new Date(), new Date(), new ArrayList<>());
                List<Range<Long>> ranges = generator.toNumberRanges(query);

                boolean flag = false;
                for (Range<Long> range : ranges) {
                    if (key >= range.getLow() && key <= range.getHigh()) {
                        flag = true;
                        break;
                    }
                }
                Assert.assertTrue(flag);
            }
        }
    }
}
