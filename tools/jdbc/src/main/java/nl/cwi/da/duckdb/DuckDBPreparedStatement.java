package nl.cwi.da.duckdb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class DuckDBPreparedStatement implements PreparedStatement {
	private DuckDBConnection conn;

	private ByteBuffer stmt_ref = null;
	private DuckDBResultSet select_result = null;
	private int update_result = 0;
	private boolean is_update = false;
	private Object[] params = null;
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

	private void prepare(String sql) throws SQLException {
		if (isClosed()) {
			throw new SQLException("Statement was closed");
		}
		if (sql == null) {
			throw new SQLException("sql query parameter cannot be null");
		}

		stmt_ref = null;
		meta = null;
		params = null;
		
		select_result = null;
		update_result = 0;
		
		stmt_ref = DuckDBNative.duckdb_jdbc_prepare(conn.conn_ref, sql);
		meta = DuckDBNative.duckdb_jdbc_meta(stmt_ref);
		params = new Object[meta.param_count];
		// TODO add query type to meta
		String query_type = DuckDBNative.duckdb_jdbc_prepare_type(stmt_ref);
		is_update = !query_type.equals("SELECT") && !query_type.equals("PRAGMA");
	}

	@Override
	public boolean execute() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Statement was closed");
		}
		if (stmt_ref == null) {
			throw new SQLException("Prepare something first");
		}
		ByteBuffer result_ref = DuckDBNative.duckdb_jdbc_execute(stmt_ref, params);
		select_result = new DuckDBResultSet(this, meta, result_ref);

		return !is_update;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		if (is_update) {
			throw new SQLException("executeQuery() can only be used with SELECT queries");
		}
		execute();
		return getResultSet();
	}

	@Override
	public int executeUpdate() throws SQLException {
		if (!is_update) {
			throw new SQLException("executeUpdate() cannot be used with SELECT queries");
		}
		execute();
		update_result = 0;
		if (select_result.next()) {
			update_result = select_result.getInt(1);
		}
		select_result.close();

		return update_result;
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
		if (stmt_ref == null || select_result == null) {
			throw new SQLException("Prepare and execute something first");
		}
		return select_result.getMetaData();
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
		params = new Object[getParameterMetaData().getParameterCount()];
	}

	@Override
	public void close() throws SQLException {
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return select_result;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return update_result;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		return DuckDBNative.duckdb_jdbc_fetch_size();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return conn == null;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
