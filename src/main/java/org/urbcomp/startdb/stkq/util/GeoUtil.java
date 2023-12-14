package org.urbcomp.startdb.stkq.util;

import org.urbcomp.startdb.stkq.model.Location;
import org.urbcomp.startdb.stkq.model.MBR;

public class GeoUtil {
    final static double R = 6371000;

    // 以m为单位
    public static MBR getMBRByCircle(Location pt, double radius) {
        double latitude = pt.getLat();
        double longitude = pt.getLon();

        double dLng = 2 * Math.asin(Math.sin(radius / (2 * R)) / Math.cos(latitude * Math.PI / 180));
        dLng = dLng * 180 / Math.PI;//角度转为弧度
        double dLat = radius / R;
        dLat = dLat * 180 / Math.PI;

        double minLat = Math.max(-90, latitude - dLat);
        double maxLat = Math.min(90, latitude + dLat);
        double minLng = Math.max(-180, longitude - dLng);
        double maxLng = Math.min(180, longitude + dLng);
        return new MBR(minLat, maxLat, minLng, maxLng);
    }
}
