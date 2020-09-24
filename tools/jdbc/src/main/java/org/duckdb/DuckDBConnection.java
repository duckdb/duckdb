package org.duckdb;

import java.nio.ByteBuffer;
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

public class DuckDBConnection implements java.sql.Connection {
	protected ByteBuffer conn_ref = null;
	protected DuckDBDatabase db;
    protected String catalog;
    protected String schema;
	public DuckDBConnection(DuckDBDatabase db) throws SQLException {
		if (db.db_ref == null) {
			throw new SQLException("Database was shutdown");
		}
		conn_ref = DuckDBNative.duckdb_jdbc_connect(db.db_ref);
		DuckDBNative.duckdb_jdbc_set_auto_commit(conn_ref, true);
		this.db = db;
		this.catalog = "catalog";
		this.schema = "schema";
	}

	public Statement createStatement() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		return new DuckDBPreparedStatement(this);
	}

	public Connection duplicate() throws SQLException {
		if (db == null) {
			throw new SQLException("Connection was closed");
		}
		if (db.db_ref == null) {
			throw new SQLException("Database was shutdown");
		}
		return new DuckDBConnection(db);
	}

	public DuckDBDatabase getDatabase() {
		return db;
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
	        System.out.format("DuckDBConnection.setTransactionIsolation %d\n", level);
			throw new SQLFeatureNotSupportedException();
		}
	}

	public int getTransactionIsolation() throws SQLException {
		return TRANSACTION_REPEATABLE_READ;
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly != db.read_only) {
	        System.out.format("DuckDBConnection.setReadOnly %s\n", readOnly);
			throw new SQLFeatureNotSupportedException("Can't change read-only status on connection level.");
		}
	}

	public boolean isReadOnly() throws SQLException {
		return db.read_only;
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
		boolean autoCommit = DuckDBNative.duckdb_jdbc_get_auto_commit(conn_ref);
		return autoCommit;
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
	    this.catalog = catalog;
	}

	public String getCatalog() throws SQLException {
	    return this.catalog;
	}

	public void setSchema(String schema) throws SQLException {
	    this.schema = schema;
	}

	public String getSchema() throws SQLException {
	    return this.schema;
	}

	public void abort(Executor executor) throws SQLException {
	    System.out.format("DuckDBConnection.abort\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Clob createClob() throws SQLException {
	    System.out.format("DuckDBConnection.createClob\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Blob createBlob() throws SQLException {
	    System.out.format("DuckDBConnection.createBlob\n");
		throw new SQLFeatureNotSupportedException();
	}

	// less likely to implement this stuff

	public <T> T unwrap(Class<T> iface) throws SQLException {
	    System.out.format("DuckDBConnection.unwrap\n");
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
	    System.out.format("DuckDBConnection.isWrapperFor\n");
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
	    System.out.format("DuckDBConnection.prepareCall\n");
		throw new SQLFeatureNotSupportedException();
	}

	public String nativeSQL(String sql) throws SQLException {
	    System.out.format("DuckDBConnection.nativeSQL %s\n", sql);
		throw new SQLFeatureNotSupportedException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
	    System.out.format("DuckDBConnection.createStatement %d %d\n", resultSetType, resultSetConcurrency);
        return createStatement();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
	    System.out.format("DuckDBConnection.prepareStatement %s %d %d\n", sql, resultSetType, resultSetConcurrency);
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
	    System.out.format("DuckDBConnection.prepareCall %s %d %d\n", sql, resultSetType, resultSetConcurrency);
		throw new SQLFeatureNotSupportedException();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
	    System.out.format("DuckDBConnection.getTypeMap\n");
		throw new SQLFeatureNotSupportedException();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	    System.out.format("DuckDBConnection.setTypeMap\n");
		throw new SQLFeatureNotSupportedException();
	}

	public void setHoldability(int holdability) throws SQLException {
	    System.out.format("DuckDBConnection.setHoldability\n");
		throw new SQLFeatureNotSupportedException();
	}

	public int getHoldability() throws SQLException {
	    System.out.format("DuckDBConnection.getHoldability\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Savepoint setSavepoint() throws SQLException {
	    System.out.format("DuckDBConnection.setSavepoint\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
	    System.out.format("DuckDBConnection.setSavepoint %s\n", name);
		throw new SQLFeatureNotSupportedException();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
	    System.out.format("DuckDBConnection.rollback\n");
		throw new SQLFeatureNotSupportedException();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	    System.out.format("DuckDBConnection.releaseSavepoint\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
	    System.out.format("DuckDBConnection.createStatement\n");
        return createStatement();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
	    System.out.format("DuckDBConnection.prepareStatement\n");
		throw new SQLFeatureNotSupportedException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
	    System.out.format("DuckDBConnection.prepareCall\n");
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
	    System.out.format("DuckDBConnection.prepareStatement\n");
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
	    System.out.format("DuckDBConnection.prepareStatement\n");
		throw new SQLFeatureNotSupportedException();
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
	    System.out.format("DuckDBConnection.prepareStatement\n");
		throw new SQLFeatureNotSupportedException();
	}

	public NClob createNClob() throws SQLException {
	    System.out.format("DuckDBConnection.createNClob\n");
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML createSQLXML() throws SQLException {
	    System.out.format("DuckDBConnection.createSQLXML\n");
		throw new SQLFeatureNotSupportedException(); // hell no
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
	    System.out.format("DuckDBConnection.setClientInfo\n");
		throw new SQLClientInfoException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
	    System.out.format("DuckDBConnection.setClientInfo\n");
		throw new SQLClientInfoException();
	}

	public String getClientInfo(String name) throws SQLException {
	    System.out.format("DuckDBConnection.getClientInfo %s\n", name);
		throw new SQLFeatureNotSupportedException();
	}

	public Properties getClientInfo() throws SQLException {
	    System.out.format("DuckDBConnection.getClientInfo\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
	    System.out.format("DuckDBConnection.createArrayOf\n");
		throw new SQLFeatureNotSupportedException();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
	    System.out.format("DuckDBConnection.createStruct\n");
		throw new SQLFeatureNotSupportedException();
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
	    System.out.format("DuckDBConnection.setNetworkTimeout\n");
		throw new SQLFeatureNotSupportedException();
	}

	public int getNetworkTimeout() throws SQLException {
	    System.out.format("DuckDBConnection.getNetworkTimeout\n");
		throw new SQLFeatureNotSupportedException();
	}

}
