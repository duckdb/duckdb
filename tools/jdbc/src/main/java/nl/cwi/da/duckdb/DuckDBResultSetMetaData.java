package nl.cwi.da.duckdb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class DuckDBResultSetMetaData implements ResultSetMetaData {

	public DuckDBResultSetMetaData(DuckDBResultSet duckDBResultSet) {
		// TODO Auto-generated constructor stub
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getColumnCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isSearchable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isCurrency(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int isNullable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isSigned(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getColumnLabel(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getColumnName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public String getSchemaName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getPrecision(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getScale(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getTableName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getCatalogName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getColumnType(int column) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getColumnTypeName(int column) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getColumnClassName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
