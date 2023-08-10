package org.duckdb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import java.util.logging.Logger;
import java.util.logging.Level;

public class DuckDBPreparedStatement implements PreparedStatement {
    private static Logger logger = Logger.getLogger(DuckDBPreparedStatement.class.getName());

    private DuckDBConnection conn;

    private ByteBuffer stmt_ref = null;
    private DuckDBResultSet select_result = null;
    private int update_result = 0;
    private boolean returnsChangedRows = false;
    private boolean returnsNothing = false;
    private boolean returnsResultSet = false;
    boolean closeOnCompletion = false;
    private Object[] params = new Object[0];
    private DuckDBResultSetMetaData meta = null;

    public DuckDBPreparedStatement(DuckDBConnection conn) throws SQLException {
        if (conn == null) {
            throw new SQLException("connection parameter cannot be null");
        }
        this.conn = conn;
    }

    public DuckDBPreparedStatement(DuckDBConnection conn, String sql) throws SQLException {
        if (conn == null) {
            throw new SQLException("connection parameter cannot be null");
        }
        if (sql == null) {
            throw new SQLException("sql query parameter cannot be null");
        }
        this.conn = conn;
        prepare(sql);
    }

    private void startTransaction() throws SQLException {
        if (this.conn.autoCommit || this.conn.transactionRunning) {
            return;
        }

        this.conn.transactionRunning = true;

        // Start transaction via Statement
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN TRANSACTION;");
        }
    }

    private void prepare(String sql) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (sql == null) {
            throw new SQLException("sql query parameter cannot be null");
        }

        // In case the statement is reused, release old one first
        if (stmt_ref != null) {
            DuckDBNative.duckdb_jdbc_release(stmt_ref);
            stmt_ref = null;
        }

        meta = null;
        params = null;

        if (select_result != null) {
            select_result.close();
        }
        select_result = null;
        update_result = 0;

        try {
            stmt_ref = DuckDBNative.duckdb_jdbc_prepare(conn.conn_ref, sql.getBytes(StandardCharsets.UTF_8));
            meta = DuckDBNative.duckdb_jdbc_prepared_statement_meta(stmt_ref);
            params = new Object[0];
        } catch (SQLException e) {
            // Delete stmt_ref as it might already be allocated
            close();
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmt_ref == null) {
            throw new SQLException("Prepare something first");
        }

        ByteBuffer result_ref = null;
        if (select_result != null) {
            select_result.close();
        }
        select_result = null;

        try {
            startTransaction();
            result_ref = DuckDBNative.duckdb_jdbc_execute(stmt_ref, params);
            DuckDBResultSetMetaData result_meta = DuckDBNative.duckdb_jdbc_query_result_meta(result_ref);
            select_result = new DuckDBResultSet(this, result_meta, result_ref, conn.conn_ref);
            returnsResultSet = result_meta.return_type.equals(StatementReturnType.QUERY_RESULT);
            returnsChangedRows = result_meta.return_type.equals(StatementReturnType.CHANGED_ROWS);
            returnsNothing = result_meta.return_type.equals(StatementReturnType.NOTHING);
        } catch (SQLException e) {
            // Delete stmt_ref as it cannot be used anymore and
            // result_ref as it might be allocated
            if (select_result != null) {
                select_result.close();
            } else if (result_ref != null) {
                DuckDBNative.duckdb_jdbc_free_result(result_ref);
                result_ref = null;
            }
            close();
            throw e;
        }

        if (returnsChangedRows) {
            if (select_result.next()) {
                update_result = select_result.getInt(1);
            }
            select_result.close();
        }

        return returnsResultSet;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        if (!returnsResultSet) {
            throw new SQLException("executeQuery() can only be used with queries that return a ResultSet");
        }
        return select_result;
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        if (!(returnsChangedRows || returnsNothing)) {
            throw new SQLException(
                "executeUpdate() can only be used with queries that return nothing (eg, a DDL statement), or update rows");
        }
        return getUpdateCountInternal();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        prepare(sql);
        return execute();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        prepare(sql);
        return executeQuery();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        prepare(sql);
        return executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (meta == null) {
            throw new SQLException("Prepare something first");
        }
        return meta;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmt_ref == null) {
            throw new SQLException("Prepare something first");
        }
        return new DuckDBParameterMetaData(meta);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > getParameterMetaData().getParameterCount()) {
            throw new SQLException("Parameter index out of bounds");
        }
        if (params.length == 0) {
            params = new Object[getParameterMetaData().getParameterCount()];
        }
        // Change sql.Timestamp to DuckDBTimestamp
        if (x instanceof Timestamp) {
            x = new DuckDBTimestamp((Timestamp) x);
        } else if (x instanceof LocalDateTime) {
            x = new DuckDBTimestamp((LocalDateTime) x);
        } else if (x instanceof OffsetDateTime) {
            x = new DuckDBTimestampTZ((OffsetDateTime) x);
        }
        params[parameterIndex - 1] = x;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void clearParameters() throws SQLException {
        params = new Object[0];
    }

    @Override
    public void close() throws SQLException {
        if (select_result != null) {
            select_result.close();
        }
        if (stmt_ref != null) {
            DuckDBNative.duckdb_jdbc_release(stmt_ref);
            stmt_ref = null;
        }
        conn = null; // we use this as a check for closed-ness
    }

    protected void finalize() throws Throwable {
        close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        logger.log(Level.FINE, "setMaxFieldSize not supported");
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        logger.log(Level.FINE, "setQueryTimeout not supported");
    }

    /**
     * This function calls the underlying C++ interrupt function which aborts the query running on that connection.
     * It is not safe to call this function when the connection is already closed.
     */
    @Override
    public synchronized void cancel() throws SQLException {
        if (conn.conn_ref != null) {
            DuckDBNative.duckdb_jdbc_interrupt(conn.conn_ref);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName");
    }

    /**
     * The returned `ResultSet` must be closed by the user to avoid a memory leak
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmt_ref == null) {
            throw new SQLException("Prepare something first");
        }

        if (!returnsResultSet) {
            return null;
        }

        // getResultSet can only be called once per result
        ResultSet to_return = select_result;
        this.select_result = null;
        return to_return;
    }

    private Integer getUpdateCountInternal() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmt_ref == null) {
            throw new SQLException("Prepare something first");
        }

        if (returnsResultSet || returnsNothing || select_result.isFinished()) {
            return -1;
        }
        return update_result;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // getUpdateCount can only be called once per result
        int to_return = getUpdateCountInternal();
        update_result = -1;
        return to_return;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction == ResultSet.FETCH_FORWARD) {
            return;
        }
        throw new SQLFeatureNotSupportedException("setFetchDirection");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return DuckDBNative.duckdb_jdbc_fetch_size();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearBatch");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeBatch");
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException("getMoreResults");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("execute");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getResultSetHoldability");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return conn == null;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException("setPoolable");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException("isPoolable");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (isClosed())
            throw new SQLException("Statement is closed");
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (isClosed())
            throw new SQLException("Statement is closed");
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
        case Types.BOOLEAN:
        case Types.BIT:
            if (x instanceof Boolean) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).byteValue() == 1);
            } else if (x instanceof String) {
                setObject(parameterIndex, Boolean.parseBoolean((String) x));
            } else {
                throw new SQLException("Can't convert value to boolean " + x.getClass().toString());
            }
            break;
        case Types.TINYINT:
            if (x instanceof Byte) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).byteValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Byte.parseByte((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (byte) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to byte " + x.getClass().toString());
            }
            break;
        case Types.SMALLINT:
            if (x instanceof Short) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).shortValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Short.parseShort((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (short) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to short " + x.getClass().toString());
            }
            break;
        case Types.INTEGER:
            if (x instanceof Integer) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).intValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Integer.parseInt((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (int) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to int " + x.getClass().toString());
            }
            break;
        case Types.BIGINT:
            if (x instanceof Long) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).longValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Long.parseLong((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (long) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to long " + x.getClass().toString());
            }
            break;
        case Types.REAL:
        case Types.FLOAT:
            if (x instanceof Float) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).floatValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Float.parseFloat((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (float) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to float " + x.getClass().toString());
            }
            break;
        case Types.DECIMAL:
            if (x instanceof BigDecimal) {
                setObject(parameterIndex, x);
            } else if (x instanceof Double) {
                setObject(parameterIndex, new BigDecimal((Double) x));
            } else if (x instanceof String) {
                setObject(parameterIndex, new BigDecimal((String) x));
            } else {
                throw new SQLException("Can't convert value to double " + x.getClass().toString());
            }
            break;
        case Types.NUMERIC:
        case Types.DOUBLE:
            if (x instanceof Double) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).doubleValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Double.parseDouble((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (double) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to double " + x.getClass().toString());
            }
            break;
        case Types.CHAR:
        case Types.LONGVARCHAR:
        case Types.VARCHAR:
            if (x instanceof String) {
                setObject(parameterIndex, (String) x);
            } else {
                setObject(parameterIndex, x.toString());
            }
            break;
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
            if (x instanceof Timestamp) {
                setObject(parameterIndex, x);
            } else if (x instanceof LocalDateTime) {
                setObject(parameterIndex, x);
            } else if (x instanceof OffsetDateTime) {
                setObject(parameterIndex, x);
            } else {
                throw new SQLException("Can't convert value to timestamp " + x.getClass().toString());
            }
            break;
        default:
            throw new SQLException("Unknown target type " + targetSqlType);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("setDate");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTime");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTimestamp");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNull");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNString");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterString");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }
}
