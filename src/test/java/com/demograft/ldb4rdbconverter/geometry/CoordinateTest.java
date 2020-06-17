package com.demograft.ldb4rdbconverter.geometry;

import org.junit.Assert;
import org.junit.Test;

public class CoordinateTest {

    @Test
    public void testDistanceTo() {
        Coordinate originalLocation = new Coordinate(58.371074d, 26.717262d);

        double distance = originalLocation.distanceTo(new Coordinate(59.356770d, 28.078634d));

        Assert.assertEquals(135_000d, distance, 1);
    }

    @Test
    public void testDirectTo() {
        Coordinate originalLocation = new Coordinate(58.371074d, 26.717262d);

        Coordinate directedLocation = originalLocation.directTo(35d, 135_000d);

        Assert.assertEquals(59.356770d, directedLocation.getLatitude(), 0.00001);
        Assert.assertEquals(28.078634d, directedLocation.getLongitude(), 0.00001);
    }
}