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

    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }

    public static double getDistance(Location pt1, Location pt2) {
        double radLat1 = rad(pt1.getLat());
        double radLat2 = rad(pt2.getLat());
        double a = radLat1 - radLat2;
        double b = rad(pt1.getLon()) - rad(pt2.getLon());
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * R;
        s = Math.round(s * 10000) / 10000.0;
        return s;
    }

    public static double getArea(Location pt1, Location pt2) {
        return Math.abs((R / 1000) * (R / 1000) * (rad(pt2.getLon()) - rad(pt1.getLon())) *
                (Math.sin(rad(pt2.getLat())) - Math.sin(rad(pt1.getLat()))));
    }
}
