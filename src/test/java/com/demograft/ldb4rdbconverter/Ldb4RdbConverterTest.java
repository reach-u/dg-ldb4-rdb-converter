package com.demograft.ldb4rdbconverter;

import org.junit.Test;

import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class Ldb4RdbConverterTest {


    private DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xx");
    @Test
    public void timeToMillisecondsConverter() {
        long milliSeconds = Ldb4RdbConverter.timeToMillisecondsConverter("2019-02-19 10:54:21.764 -0500", timeFormatter);
        assertEquals(1550591661764L, milliSeconds);
    }
}