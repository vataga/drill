package org.apache.drill.common.util;

import java.time.format.DateTimeFormatterBuilder;


/**
 * Extends regular {@link java.time.Instant#parse} with more formats.
 * By default, {@link java.time.format.DateTimeFormatter#ISO_INSTANT} used.
 */
public class DrillDateTimeFormatter {
  public static java.time.format.DateTimeFormatter ISO_DATETIME_FORMATTER =
    new DateTimeFormatterBuilder().append(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .optionalStart().appendOffset("+HH:MM", "+00:00").optionalEnd()
    .optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
    .optionalStart().appendOffset("+HH", "Z").optionalEnd()
    .toFormatter();
}
