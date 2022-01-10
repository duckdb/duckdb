package org.duckdb;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class DuckDBTimestamp {
    static 
    {
        // LocalDateTime reference of epoch
        RefLocalDateTime = 
            LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
    }

    public DuckDBTimestamp(long timeMicros) {
        this.timeMicros = timeMicros;
    }

    public DuckDBTimestamp(LocalDateTime localDateTime) {
        this.timeMicros = DuckDBTimestamp.RefLocalDateTime.until(
            localDateTime, ChronoUnit.MICROS);
    }

    public DuckDBTimestamp(Timestamp sqlTimestamp) {
        this.timeMicros = DuckDBTimestamp.RefLocalDateTime.until(
            sqlTimestamp.toLocalDateTime(), ChronoUnit.MICROS);
    }

    final static LocalDateTime RefLocalDateTime;
    protected long timeMicros;

    public static Timestamp toSqlTimestamp(long timeMicros) {
        return Timestamp.valueOf(
            LocalDateTime.ofEpochSecond(micros2seconds(timeMicros)
                , nanosPartMicros(timeMicros), ZoneOffset.UTC));
    }
    
    public static LocalDateTime toLocalDateTime(long timeMicros) {
        return LocalDateTime.ofEpochSecond(micros2seconds(timeMicros)
                , nanosPartMicros(timeMicros), ZoneOffset.UTC);
    }

    public Timestamp toSqlTimestamp() {
        return Timestamp.valueOf(this.toLocalDateTime());
    }

    public LocalDateTime toLocalDateTime() {
        return LocalDateTime.ofEpochSecond(micros2seconds(timeMicros)
                , nanosPartMicros(timeMicros), ZoneOffset.UTC);
    }

    public static long getMicroseconds(Timestamp sqlTimestamp) {
        return DuckDBTimestamp.RefLocalDateTime.until(
                sqlTimestamp.toLocalDateTime(), ChronoUnit.MICROS);
    }

    public long getMicrosEpoch() {
        return this.timeMicros;
    }

    public String toString() {
        return this.toLocalDateTime().toString();
    }

    private static long micros2seconds(long micros) {
        return micros / 1000_000L;
    }

    private static int nanosPartMicros(long micros) {
        return (int) ((micros % 1000_000L) * 1000);
    }
}
