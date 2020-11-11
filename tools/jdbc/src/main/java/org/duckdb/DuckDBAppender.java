package org.duckdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class DuckDBAppender implements AutoCloseable {

    protected ByteBuffer appender_ref = null;

    public DuckDBAppender(DuckDBConnection con, String schemaName, String tableName) throws SQLException {
        if (con == null) {
            throw new SQLException("Invalid connection");
        }
        appender_ref = DuckDBNative.duckdb_jdbc_create_appender(con.conn_ref, schemaName.getBytes(StandardCharsets.UTF_8), tableName.getBytes(StandardCharsets.UTF_8));
    }

    public void beginRow() {
        DuckDBNative.duckdb_jdbc_appender_begin_row(appender_ref);
    }

    public void endRow() {
        DuckDBNative.duckdb_jdbc_appender_end_row(appender_ref);
    }

    public void flush() {
        DuckDBNative.duckdb_jdbc_appender_flush(appender_ref);
    }

    public void append(int value) {
        DuckDBNative.duckdb_jdbc_appender_append_int(appender_ref, value);
    }

    protected void finalize() throws Throwable {
        close();
    }

    public synchronized void close() throws SQLException {
        if (appender_ref != null) {
            DuckDBNative.duckdb_jdbc_appender_close(appender_ref);
            appender_ref = null;
        }
    }

}
