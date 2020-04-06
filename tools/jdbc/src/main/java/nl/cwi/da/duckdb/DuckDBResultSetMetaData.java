package nl.cwi.da.duckdb;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class DuckDBResultSetMetaData implements ResultSetMetaData {

	public DuckDBResultSetMetaData(int column_count, String[] column_names, String[] column_types) {
		this.column_count = column_count;
		this.column_names = column_names;
		this.column_types = column_types;
	}

	public DuckDBResultSetMetaData() {
		// TODO Auto-generated constructor stub
	}

	private int column_count;
	private String[] column_names;
	private String[] column_types;

	public int getColumnCount() throws SQLException {
		return column_count;
	}

	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}

	public String getColumnName(int column) throws SQLException {
		if (column > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return column_names[column];
	}

	public int getColumnType(int column) throws SQLException {
		String type_name = getColumnTypeName(column);
		switch (type_name) {
		case "INTEGER":
			return Types.INTEGER;
		default:
			throw new SQLException("Unknown type " + type_name);
		}
	}

	public String getColumnTypeName(int column) throws SQLException {
		if (column > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return column_types[column];
	}

	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	public String getSchemaName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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

	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getColumnClassName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
