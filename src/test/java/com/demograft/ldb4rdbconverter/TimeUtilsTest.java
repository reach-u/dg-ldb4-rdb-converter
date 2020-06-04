package com.demograft.ldb4rdbconverter;

import org.junit.Test;

import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class TimeUtilsTest {


    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xx");

    @Test
    public void timeToMillisecondsConverter() {
        long milliSeconds = TimeUtils.timeToMillisecondsConverter("2019-02-19 10:54:21.764 -0500", timeFormatter, false, "Z");
        assertEquals(1550591661764L, milliSeconds);
    }

    @Test
    public void timeInMsEpochToMillisecondsConverter() {
        long milliSeconds = TimeUtils.timeToMillisecondsConverter("1589414444", null, true, "Z");
        assertEquals(1589414444L, milliSeconds);
    }

    @Test
    public void timeInSecondsEpochToMillisecondsConverter() {
        long milliSeconds = TimeUtils.timeToMillisecondsConverter("1589414444", null, false, "Z");
        assertEquals(1589414444000L, milliSeconds);
    }
}