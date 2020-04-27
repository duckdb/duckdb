package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
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

public class DuckDBConnection implements java.sql.Connection {
	protected ByteBuffer conn_ref = null;
	protected DuckDBDatabase db;

	public DuckDBConnection(DuckDBDatabase db) {
		conn_ref = DuckDBNative.duckdb_jdbc_connect(db.db_ref);
		DuckDBNative.duckdb_jdbc_set_auto_commit(conn_ref, true);
		this.db = db;
	}

	public Statement createStatement() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		return new DuckDBPreparedStatement(this);
	}

	public void commit() throws SQLException {
		Statement s = createStatement();
		s.execute("COMMIT");
		s.close();
	}

	public void rollback() throws SQLException {
		Statement s = createStatement();
		s.execute("ROLLBACK");
		s.close();
	}

	protected void finalize() throws Throwable {
		close();
	}

	public synchronized void close() throws SQLException {
		if (conn_ref != null) {
			DuckDBNative.duckdb_jdbc_disconnect(conn_ref);
			conn_ref = null;
		}
		db = null;
	}

	public boolean isClosed() throws SQLException {
		return conn_ref == null;
	}

	public boolean isValid(int timeout) throws SQLException {
		if (isClosed()) {
			return false;
		}
		// run a query just to be sure
		Statement s = createStatement();
		ResultSet rs = s.executeQuery("SELECT 42");
		if (!rs.next() || rs.getInt(1) != 42) {
			rs.close();
			s.close();
			return false;
		}
		rs.close();
		s.close();

		return true;
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void clearWarnings() throws SQLException {
	}

	public void setTransactionIsolation(int level) throws SQLException {
		if (level > TRANSACTION_REPEATABLE_READ) {
			throw new SQLFeatureNotSupportedException();
		}
	}

	public int getTransactionIsolation() throws SQLException {
		return TRANSACTION_REPEATABLE_READ;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly) {
			throw new SQLFeatureNotSupportedException("Invidual connections can't be read-only");
		}
	}

	public boolean isReadOnly() throws SQLException {
		return false;
	}

	// at some point we will implement this

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		DuckDBNative.duckdb_jdbc_set_auto_commit(conn_ref, autoCommit);
	}

	public boolean getAutoCommit() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		return DuckDBNative.duckdb_jdbc_get_auto_commit(conn_ref);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		return new DuckDBPreparedStatement(this, sql);
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return new DuckDBDatabaseMetaData(this);
	}

	public void setCatalog(String catalog) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getCatalog() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	// less likely to implement this stuff

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String nativeSQL(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
			return createStatement();
		}
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException(); // hell no
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
