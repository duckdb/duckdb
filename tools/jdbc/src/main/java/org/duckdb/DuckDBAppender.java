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
        appender_ref = DuckDBNative.duckdb_jdbc_create_appender(
            con.conn_ref, schemaName.getBytes(StandardCharsets.UTF_8), tableName.getBytes(StandardCharsets.UTF_8));
    }

    public void beginRow() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_begin_row(appender_ref);
    }

    public void endRow() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_end_row(appender_ref);
    }

    public void flush() throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_flush(appender_ref);
    }

    public void append(boolean value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_boolean(appender_ref, value);
    }

    public void append(byte value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_byte(appender_ref, value);
    }

    public void append(short value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_short(appender_ref, value);
    }

    public void append(int value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_int(appender_ref, value);
    }

    public void append(long value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_long(appender_ref, value);
    }

    public void append(float value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_float(appender_ref, value);
    }

    public void append(double value) throws SQLException {
        DuckDBNative.duckdb_jdbc_appender_append_double(appender_ref, value);
    }

    public void append(String value) throws SQLException {
        if (value == null) {
            DuckDBNative.duckdb_jdbc_appender_append_null(appender_ref);
        } else {
            DuckDBNative.duckdb_jdbc_appender_append_string(appender_ref, value.getBytes(StandardCharsets.UTF_8));
        }
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
