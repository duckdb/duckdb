package org.duckdb;

import java.time.OffsetDateTime;

public class DuckDBTimestampTZ extends DuckDBTimestamp {

    public DuckDBTimestampTZ(long timeMicros) {
        super(timeMicros);
    }

    public DuckDBTimestampTZ(OffsetDateTime offsetDateTime) {
        super(offsetDateTime);
    }
}
