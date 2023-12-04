package org.duckdb;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public final class DuckDBConnection implements java.sql.Connection {

    /** Name of the DuckDB default schema. */
    public static final String DEFAULT_SCHEMA = "main";

    ByteBuffer conn_ref;
    boolean autoCommit = true;
    boolean transactionRunning;
    final String url;
    private final boolean readOnly;

    public static DuckDBConnection newConnection(String url, boolean readOnly, Properties properties)
        throws SQLException {
        if (!url.startsWith("jdbc:duckdb")) {
            throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
        }
        String db_dir = url.substring("jdbc:duckdb:".length()).trim();
        if (db_dir.length() == 0) {
            db_dir = ":memory:";
        }
        ByteBuffer nativeReference =
            DuckDBNative.duckdb_jdbc_startup(db_dir.getBytes(StandardCharsets.UTF_8), readOnly, properties);
        return new DuckDBConnection(nativeReference, url, readOnly);
    }

    private DuckDBConnection(ByteBuffer connectionReference, String url, boolean readOnly) throws SQLException {
        conn_ref = connectionReference;
        this.url = url;
        this.readOnly = readOnly;
        DuckDBNative.duckdb_jdbc_set_auto_commit(connectionReference, true);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            return new DuckDBPreparedStatement(this);
        }
        throw new SQLFeatureNotSupportedException("createStatement");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            return new DuckDBPreparedStatement(this, sql);
        }
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public Connection duplicate() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
        return new DuckDBConnection(DuckDBNative.duckdb_jdbc_connect(conn_ref), url, readOnly);
    }

    public void commit() throws SQLException {
        try (Statement s = createStatement()) {
            s.execute("COMMIT");
            transactionRunning = false;
        }
    }

    public void rollback() throws SQLException {
        try (Statement s = createStatement()) {
            s.execute("ROLLBACK");
            transactionRunning = false;
        }
    }

    protected void finalize() throws Throwable {
        close();
    }

    public synchronized void close() throws SQLException {
        if (conn_ref != null) {
            DuckDBNative.duckdb_jdbc_disconnect(conn_ref);
            conn_ref = null;
        }
    }

    public boolean isClosed() throws SQLException {
        return conn_ref == null;
    }

    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        // run a query just to be sure
        try (Statement s = createStatement(); ResultSet rs = s.executeQuery("SELECT 42")) {
            return rs.next() && rs.getInt(1) == 42;
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (level > TRANSACTION_REPEATABLE_READ) {
            throw new SQLFeatureNotSupportedException("setTransactionIsolation");
        }
    }

    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_REPEATABLE_READ;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly != this.readOnly) {
            throw new SQLFeatureNotSupportedException("Can't change read-only status on connection level.");
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }

        if (this.autoCommit != autoCommit) {
            this.autoCommit = autoCommit;

            // A running transaction is committed if switched to auto-commit
            if (transactionRunning && autoCommit) {
                this.commit();
            }
        }
        return;

        // Native method is not working as one would expect ... uncomment maybe later
        // DuckDBNative.duckdb_jdbc_set_auto_commit(conn_ref, autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        return this.autoCommit;

        // Native method is not working as one would expect ... uncomment maybe later
        // return DuckDBNative.duckdb_jdbc_get_auto_commit(conn_ref);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new DuckDBDatabaseMetaData(this);
    }

    public void setCatalog(String catalog) throws SQLException {
        DuckDBNative.duckdb_jdbc_set_catalog(conn_ref, catalog);
    }

    public String getCatalog() throws SQLException {
        return DuckDBNative.duckdb_jdbc_get_catalog(conn_ref);
    }

    public void setSchema(String schema) throws SQLException {
        DuckDBNative.duckdb_jdbc_set_schema(conn_ref, schema);
    }

    public String getSchema() throws SQLException {
        return DuckDBNative.duckdb_jdbc_get_schema(conn_ref);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("abort");
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    // less likely to implement this stuff

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("nativeSQL");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, 0);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, 0);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("setHoldability");
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML"); // hell no
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout");
    }

    public DuckDBAppender createAppender(String schemaName, String tableName) throws SQLException {
        return new DuckDBAppender(this, schemaName, tableName);
    }

    private static long getArrowStreamAddress(Object arrow_array_stream) {
        try {
            Class<?> arrow_array_stream_class = Class.forName("org.apache.arrow.c.ArrowArrayStream");
            if (!arrow_array_stream_class.isInstance(arrow_array_stream)) {
                throw new RuntimeException("Need to pass an ArrowArrayStream");
            }
            return (Long) arrow_array_stream_class.getMethod("memoryAddress").invoke(arrow_array_stream);

        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerArrowStream(String name, Object arrow_array_stream) {
        long array_stream_address = getArrowStreamAddress(arrow_array_stream);
        DuckDBNative.duckdb_jdbc_arrow_register(conn_ref, array_stream_address, name.getBytes(StandardCharsets.UTF_8));
    }
}
