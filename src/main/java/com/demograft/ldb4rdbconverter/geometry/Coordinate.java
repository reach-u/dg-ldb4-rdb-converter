package com.demograft.ldb4rdbconverter.geometry;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import net.sf.geographiclib.Geodesic;
import net.sf.geographiclib.GeodesicData;
import net.sf.geographiclib.GeodesicLine;
import net.sf.geographiclib.GeodesicMask;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Coordinate {
    private final double latitude;
    private final double longitude;

    private static final Geodesic geodesic = Geodesic.WGS84;

    public double distanceTo(Coordinate other) {
        GeodesicLine line = geodesic.InverseLine(this.latitude, this.longitude,
                other.latitude, other.longitude,
                GeodesicMask.DISTANCE_IN | GeodesicMask.LATITUDE | GeodesicMask.LONGITUDE);
        return line.Distance();
    }

    public Coordinate directTo(double azimuth, double distance) {
        GeodesicData geodesicData = geodesic.Direct(this.latitude, this.longitude, azimuth, distance);
        return new Coordinate(geodesicData.lat2, geodesicData.lon2);
    }
}
