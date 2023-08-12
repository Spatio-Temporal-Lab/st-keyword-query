package org.urbcomp.startdb.stkq.keyGenerator;

import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.util.DateUtil;
import junit.framework.TestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class TimeSpatialKeyGeneratorTest extends TestCase {
    TimeKeyGenerator timeKeyGenerator = new TimeKeyGenerator();
    SpatialKeyGenerator spatialKeyGenerator = new SpatialKeyGenerator();
    public void testToKeyRanges() throws ParseException {
        Date date = DateUtil.getDate("2010-06-06 12:00:00");
        Date left = DateUtil.getDate("2010-06-06 11:00:00");
        Date right = DateUtil.getDate("2010-06-06 13:00:00");
        Location location = new Location(9.995, 9.995);
        Location leftUp = new Location(9.99, 9.99);
        Location rightDown = new Location(10.00, 10.00);
        Query query = new Query(leftUp.getLat(), rightDown.getLat(), leftUp.getLon(), rightDown.getLon(), left, right, new ArrayList<>());
        System.out.println(Arrays.toString(timeKeyGenerator.toKey(date)));
        System.out.println(Arrays.toString(spatialKeyGenerator.toKey(location)));
    }
}
