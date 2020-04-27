package nl.cwi.da.duckdb;

import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

public class DuckDBResultSetMetaData implements ResultSetMetaData {

	public DuckDBResultSetMetaData(int param_count, int column_count, String[] column_names, String[] column_types) {
		this.param_count = param_count;
		this.column_count = column_count;
		this.column_names = column_names;
		this.column_types = column_types;
	}

	protected int param_count;
	protected int column_count;
	protected String[] column_names;
	protected String[] column_types;

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
		return column_names[column - 1];
	}

	public int getColumnType(int column) throws SQLException {
		String type_name = getColumnTypeName(column);
		switch (type_name) {
		case "BOOLEAN":
			return Types.BOOLEAN;
		case "TINYINT":
			return Types.TINYINT;
		case "SMALLINT":
			return Types.SMALLINT;
		case "INTEGER":
			return Types.INTEGER;
		case "BIGINT":
			return Types.BIGINT;
		case "FLOAT":
			return Types.FLOAT;
		case "DOUBLE":
			return Types.DOUBLE;
		case "VARCHAR":
			return Types.VARCHAR;
		default:
			throw new SQLException("Unknown type " + type_name);
		}
	}

	public String getColumnClassName(int column) throws SQLException {
		switch (getColumnType(column)) {
		case Types.BOOLEAN:
			return Boolean.class.toString();
		case Types.TINYINT:
			return Byte.class.toString();
		case Types.SMALLINT:
			return Short.class.toString();
		case Types.INTEGER:
			return Integer.class.toString();
		case Types.BIGINT:
			return Long.class.toString();
		case Types.FLOAT:
			return Float.class.toString();
		case Types.DOUBLE:
			return Double.class.toString();
		case Types.VARCHAR:
			return String.class.toString();
		default:
			throw new SQLException("Unknown type " + getColumnTypeName(column));
		}
	}

	public String getColumnTypeName(int column) throws SQLException {
		if (column > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return column_types[column - 1];
	}

	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {
		return false;
	}

	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	public int isNullable(int column) throws SQLException {
		return columnNullable;
	}

	public String getSchemaName(int column) throws SQLException {
		return "";
	}

	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	public boolean isSearchable(int column) throws SQLException {
		return true;
	}

	public boolean isCurrency(int column) throws SQLException {
		return false;
	}

	public boolean isSigned(int column) throws SQLException {
		return false;
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getPrecision(int column) throws SQLException {
		return 0;
	}

	public int getScale(int column) throws SQLException {
		return 0;
	}

	public String getTableName(int column) throws SQLException {
		return "";
	}

	public String getCatalogName(int column) throws SQLException {
		return "";
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
