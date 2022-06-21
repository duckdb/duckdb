package org.duckdb;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.time.OffsetDateTime;
import java.math.BigDecimal;

import org.duckdb.DuckDBResultSet.DuckDBBlobResult;

public class DuckDBResultSetMetaData implements ResultSetMetaData {

	public DuckDBResultSetMetaData(int param_count, int column_count, String[] column_names,
			String[] column_types_string, String[] column_types_details) {
		this.param_count = param_count;
		this.column_count = column_count;
		this.column_names = column_names;
		this.column_types_string = column_types_string;
		this.column_types_details = column_types_details;
		ArrayList<DuckDBColumnType> column_types_al = new ArrayList<DuckDBColumnType>(column_count);
		ArrayList<DuckDBColumnTypeMetaData> column_types_meta = new ArrayList<DuckDBColumnTypeMetaData>(column_count);

		for (String column_type_string : this.column_types_string) {
			column_types_al.add(TypeNameToType(column_type_string));
		}
		this.column_types = new DuckDBColumnType[column_count];
		this.column_types = column_types_al.toArray(this.column_types);

		for (String column_type_detail : this.column_types_details) {
			if (!column_type_detail.equals("")) {
				String[] split_details = column_type_detail.split(";");
				column_types_meta.add(new DuckDBColumnTypeMetaData(Short.parseShort(split_details[0].replace("DECIMAL", ""))
							, Short.parseShort(split_details[1]), Short.parseShort(split_details[2])));
			}
			else { column_types_meta.add(null); }
		}
		this.column_types_meta = column_types_meta.toArray(new DuckDBColumnTypeMetaData[column_count]);
	}

	public static DuckDBColumnType TypeNameToType(String type_name) {
		if (type_name.startsWith("DECIMAL")) {
			return DuckDBColumnType.DECIMAL;
		} else if (type_name.equals("TIMESTAMP WITH TIME ZONE")) {
			return DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE;
		} else {
			return DuckDBColumnType.valueOf(type_name);
		}
	}

	protected int param_count;
	protected int column_count;
	protected String[] column_names;
	protected String[] column_types_string;
	protected String[] column_types_details;
	protected DuckDBColumnType[] column_types;
	protected DuckDBColumnTypeMetaData[] column_types_meta;

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

	public static int type_to_int(DuckDBColumnType type) throws SQLException {
		switch (type) {
		case BOOLEAN:
			return Types.BOOLEAN;
		case TINYINT:
			return Types.TINYINT;
		case SMALLINT:
			return Types.SMALLINT;
		case INTEGER:
			return Types.INTEGER;
		case BIGINT:
			return Types.BIGINT;
		case HUGEINT:
			return Types.JAVA_OBJECT;
		case UTINYINT:
			return Types.JAVA_OBJECT;
		case USMALLINT:
			return Types.JAVA_OBJECT;
		case UINTEGER:
			return Types.JAVA_OBJECT;
		case UBIGINT:
			return Types.JAVA_OBJECT;
		case FLOAT:
			return Types.FLOAT;
		case DOUBLE:
			return Types.DOUBLE;
		case DECIMAL:
			return Types.DECIMAL;
		case VARCHAR:
			return Types.VARCHAR;
		case TIME:
			return Types.TIME;
		case DATE:
			return Types.DATE;
		case TIMESTAMP:
			return Types.TIMESTAMP;
		case TIMESTAMP_WITH_TIME_ZONE:
			return Types.TIME_WITH_TIMEZONE;
		case INTERVAL:
			return Types.JAVA_OBJECT;
		case BLOB:
			return Types.BLOB;

		default:
			throw new SQLException("Unsupported type " + type.toString());
		}
	}

	public int getColumnType(int column) throws SQLException {
		if (column > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return type_to_int(column_types[column - 1]);
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
		case Types.TIME_WITH_TIMEZONE:
			return OffsetDateTime.class.toString();
		case Types.BLOB:
			return DuckDBBlobResult.class.toString();
		case Types.DECIMAL:
			return BigDecimal.class.toString();
		default:
			throw new SQLException("Unknown type " + getColumnTypeName(column));
		}
	}

	public String getColumnTypeName(int column) throws SQLException {
		if (column > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return column_types_string[column - 1];
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
		DuckDBColumnTypeMetaData typeMetaData = typeMetadataForColumn(column);

		if (typeMetaData == null) {
			return 0;
		}

		return typeMetaData.width;
	}

	public int getScale(int column) throws SQLException {
		DuckDBColumnTypeMetaData typeMetaData = typeMetadataForColumn(column);

		if (typeMetaData == null) {
			return 0;
		}

		return typeMetaData.scale;
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

	private DuckDBColumnTypeMetaData typeMetadataForColumn(int columnIndex) throws SQLException {
		if (columnIndex > column_count) {
			throw new SQLException("Column index out of bounds");
		}
		return column_types_meta[columnIndex - 1];
	}
}
