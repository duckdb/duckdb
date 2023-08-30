package org.duckdb;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoUnit;

public class DuckDBTimestamp {
    static {
        // LocalDateTime reference of epoch
        RefLocalDateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
    }

    public DuckDBTimestamp(long timeMicros) {
        this.timeMicros = timeMicros;
    }

    public DuckDBTimestamp(LocalDateTime localDateTime) {
        this.timeMicros = DuckDBTimestamp.RefLocalDateTime.until(localDateTime, ChronoUnit.MICROS);
    }

    public DuckDBTimestamp(OffsetDateTime offsetDateTime) {
        this.timeMicros = DuckDBTimestamp.RefLocalDateTime.until(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC),
                                                                 ChronoUnit.MICROS);
    }

    public DuckDBTimestamp(Timestamp sqlTimestamp) {
        this.timeMicros = DuckDBTimestamp.RefLocalDateTime.until(sqlTimestamp.toLocalDateTime(), ChronoUnit.MICROS);
    }

    final static LocalDateTime RefLocalDateTime;
    protected long timeMicros;

    public static Timestamp toSqlTimestamp(long timeMicros) {
        return Timestamp.valueOf(
            LocalDateTime.ofEpochSecond(micros2seconds(timeMicros), nanosPartMicros(timeMicros), ZoneOffset.UTC));
    }

    public static Timestamp toSqlTimestampNanos(long timeNanos) {
        return Timestamp.valueOf(
            LocalDateTime.ofEpochSecond(nanos2seconds(timeNanos), nanosPartNanos(timeNanos), ZoneOffset.UTC));
    }

    public static LocalDateTime toLocalDateTime(long timeMicros) {
        return LocalDateTime.ofEpochSecond(micros2seconds(timeMicros), nanosPartMicros(timeMicros), ZoneOffset.UTC);
    }

    public static OffsetTime toOffsetTime(long timeBits) {
        long timeMicros = timeBits >> 24;   // High 40 bits are micros
        long offset = timeBits & 0x0FFFFFF; // Low 24 bits are biased offset in seconds
        offset -= 1559 * 60 * 60;
        int sign = (offset < 0) ? -1 : 1;
        offset = Math.abs(offset);

        int ss = (int) offset % 60;
        offset = offset / 60;

        int mm = (int) offset % 60;
        int hh = (int) offset / 60;

        if (hh > 18) {
            return OffsetTime.of(toLocalTime(timeMicros), ZoneOffset.UTC);
        } else {
            return OffsetTime.of(toLocalTime(timeMicros),
                                 ZoneOffset.ofHoursMinutesSeconds(sign * hh, sign * mm, sign * ss));
        }
    }

    private static LocalTime toLocalTime(long timeMicros) {
        return LocalTime.ofNanoOfDay(timeMicros * 1000);
    }

    public static OffsetDateTime toOffsetDateTime(long timeMicros) {
        return OffsetDateTime.of(toLocalDateTime(timeMicros), ZoneOffset.UTC);
    }

    public static Timestamp fromSecondInstant(long seconds) {
        return fromMilliInstant(seconds * 1_000);
    }

    public static Timestamp fromMilliInstant(long millis) {
        return new Timestamp(millis);
    }

    public static Timestamp fromMicroInstant(long micros) {
        return Timestamp.from(Instant.ofEpochSecond(micros / 1_000_000, nanosPartMicros(micros)));
    }

    public static Timestamp fromNanoInstant(long nanos) {
        return Timestamp.from(Instant.ofEpochSecond(nanos / 1_000_000_000, nanosPartNanos(nanos)));
    }

    public Timestamp toSqlTimestamp() {
        return Timestamp.valueOf(this.toLocalDateTime());
    }

    public LocalDateTime toLocalDateTime() {
        return LocalDateTime.ofEpochSecond(micros2seconds(timeMicros), nanosPartMicros(timeMicros), ZoneOffset.UTC);
    }

    public OffsetDateTime toOffsetDateTime() {
        return OffsetDateTime.of(toLocalDateTime(this.timeMicros), ZoneOffset.UTC);
    }

    public static long getMicroseconds(Timestamp sqlTimestamp) {
        return DuckDBTimestamp.RefLocalDateTime.until(sqlTimestamp.toLocalDateTime(), ChronoUnit.MICROS);
    }

    public long getMicrosEpoch() {
        return this.timeMicros;
    }

    public String toString() {
        return this.toLocalDateTime().toString();
    }

    private static long micros2seconds(long micros) {
        if ((micros % 1000_000L) >= 0) {
            return micros / 1000_000L;
        } else {
            return (micros / 1000_000L) - 1;
        }
    }

    private static int nanosPartMicros(long micros) {
        if ((micros % 1000_000L) >= 0) {
            return (int) ((micros % 1000_000L) * 1000);
        } else {
            return (int) ((1000_000L + (micros % 1000_000L)) * 1000);
        }
    }

    private static long nanos2seconds(long nanos) {
        if ((nanos % 1_000_000_000L) >= 0) {
            return nanos / 1_000_000_000L;
        } else {
            return (nanos / 1_000_000_000L) - 1;
        }
    }

    private static int nanosPartNanos(long nanos) {
        if ((nanos % 1_000_000_000L) >= 0) {
            return (int) ((nanos % 1_000_000_000L));
        } else {
            return (int) ((1_000_000_000L + (nanos % 1_000_000_000L)));
        }
    }
}
