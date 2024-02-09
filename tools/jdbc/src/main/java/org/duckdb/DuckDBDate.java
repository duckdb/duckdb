package org.duckdb;

import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DuckDBDate {
    private final long daysSinceEpoch;

    public DuckDBDate(Date date) {
        this.daysSinceEpoch = LocalDate.ofEpochDay(0).until(date.toLocalDate(), ChronoUnit.DAYS);
    }

    public long getDaysSinceEpoch() {
        return daysSinceEpoch;
    }
}
