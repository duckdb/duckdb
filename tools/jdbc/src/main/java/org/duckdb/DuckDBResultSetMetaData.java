package org.duckdb;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
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
		if (type_name.equals("BOOLEAN")) {
			return Types.BOOLEAN;
		} else if (type_name.equals("TINYINT")) {
			return Types.TINYINT;
		} else if (type_name.equals("SMALLINT")) {
			return Types.SMALLINT;
		} else if (type_name.equals("INTEGER")) {
			return Types.INTEGER;
		} else if (type_name.equals("BIGINT")) {
			return Types.BIGINT;
		} else if (type_name.equals("FLOAT")) {
			return Types.FLOAT;
		} else if (type_name.equals("DOUBLE") || type_name.startsWith("DECIMAL")) {
			return Types.DOUBLE;
		} else if (type_name.equals("VARCHAR")) {
			return Types.VARCHAR;
		}  else if (type_name.equals("TIME")) {
			return Types.TIME;
		}  else if (type_name.equals("DATE")) {
			return Types.DATE;
		}  else if (type_name.equals("TIMESTAMP")) {
			return Types.TIMESTAMP;
		}  else if (type_name.equals("INTERVAL")) {
			return Types.VARCHAR;
		} else {
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
		case Types.TIME:
			return Time.class.toString();
		case Types.DATE:
			return Date.class.toString();
		case Types.TIMESTAMP:
			return Timestamp.class.toString();
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
