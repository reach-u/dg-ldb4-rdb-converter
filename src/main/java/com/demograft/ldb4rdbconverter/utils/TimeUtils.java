package com.demograft.ldb4rdbconverter.utils;

import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@UtilityClass
public class TimeUtils {

    /**
     * method tries to guess input date formatter.
     *
     * @param time     input time string
     * @param timeZone timezone for case where input time doesn't have an timeZone given.
     * @return DateTimeFormetter or null if the input time is an epoch.
     */
    public DateTimeFormatter identifyTimeFormat(String time, String timeZone) {
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xx");
            ZonedDateTime.parse(time, formatter);
            return formatter;
        } catch (DateTimeParseException e) {
            // Intentionally empty
        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

            ZonedDateTime.parse(time, formatter);
            return formatter;
        } catch (DateTimeParseException e2) {
            // Intentionally empty
        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_ZONED_DATE_TIME;

            ZonedDateTime.parse(time, formatter);
            return formatter;
        } catch (DateTimeParseException e1) {
            // Intentionally empty
        }
        try {
            Long.parseLong(time);
            return null;
        } catch (NumberFormatException e1) {
            // Intentionally empty
        }
        try {
            DateTimeFormatter formatter
                    = DateTimeFormatter.ISO_LOCAL_DATE_TIME;


            LocalDateTime localdate = LocalDateTime.parse(time, formatter);
            ZonedDateTime.of(localdate, ZoneId.of(timeZone));
            return formatter;
        } catch (DateTimeParseException e3) {
            throw new RuntimeException("Error. Couldn't get DateTime parser from first example row. Does not match any common date time formats.");
        }
    }


    public long timeToMillisecondsConverter(String time, DateTimeFormatter format, boolean inputEpochInMilliseconds, String timeZone) {
        if (format != null) {
            try {
                ZonedDateTime date = ZonedDateTime.parse(time, format);
                return date.toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                LocalDateTime localdate = LocalDateTime.parse(time, format);
                ZonedDateTime date = ZonedDateTime.of(localdate, ZoneId.of(timeZone));
                return date.toInstant().toEpochMilli();
            }
        } else {
            try {
                if (time.length() < 10) {
                    throw new DateTimeParseException("Couldn't parse input time to long", time, 0);
                }

                long epoch = Long.parseLong(time);
                if (inputEpochInMilliseconds) {
                    return epoch;
                } else {
                    return epoch * 1000;
                }

            } catch (NumberFormatException e) {
                throw new DateTimeParseException("Couldn't parse input time to long", time, 0, e);
            }
        }
    }
}
