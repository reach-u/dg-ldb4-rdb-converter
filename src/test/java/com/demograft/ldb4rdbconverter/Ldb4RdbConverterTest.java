package com.demograft.ldb4rdbconverter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Ldb4RdbConverterTest {


    @Test
    public void timeToMillisecondsConverter() {
        long milliSeconds = Ldb4RdbConverter.timeToMillisecondsConverter("2019-02-19 10:54:21.764 -0500");
        assertEquals(1550591661764L, milliSeconds);
    }
}