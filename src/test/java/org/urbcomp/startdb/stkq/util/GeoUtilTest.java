package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.model.Location;
import junit.framework.TestCase;

public class GeoUtilTest extends TestCase {

    public void testGetMBRByCircle() {
    }

    public void testGetDistance() {
    }

    public void testGetArea() {
        Location pt1 = new Location(38.5, -90.1);
        Location pt2 = new Location(38.6, -90.0);
        System.out.println(GeoUtil.getArea(pt1, pt2));
        Location pt3 = new Location(38.50, -90.10);
        Location pt4 = new Location(38.51, -90.09);
        System.out.println(GeoUtil.getArea(pt3, pt4));
    }
}
