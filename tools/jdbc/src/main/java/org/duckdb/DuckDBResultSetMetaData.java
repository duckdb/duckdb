package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class DuckDBResultSetMetaData implements ResultSetMetaData {

    public DuckDBResultSetMetaData(int param_count, int column_count, String[] column_names,
                                   String[] column_types_string, String[] column_types_details, String return_type) {
        this.param_count = param_count;
        this.column_count = column_count;
        this.column_names = column_names;
        this.return_type = StatementReturnType.valueOf(return_type);
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
            if (column_type_detail.startsWith("DECIMAL")) {
                column_types_meta.add(DuckDBColumnTypeMetaData.parseColumnTypeMetadata(column_type_detail));
            } else {
                column_types_meta.add(null);
            }
        }
        this.column_types_meta = column_types_meta.toArray(new DuckDBColumnTypeMetaData[column_count]);
    }

    public static DuckDBColumnType TypeNameToType(String type_name) {
        if (type_name.endsWith("[]")) {
            return DuckDBColumnType.LIST;
        } else if (type_name.startsWith("DECIMAL")) {
            return DuckDBColumnType.DECIMAL;
        } else if (type_name.equals("TIME WITH TIME ZONE")) {
            return DuckDBColumnType.TIME_WITH_TIME_ZONE;
        } else if (type_name.equals("TIMESTAMP WITH TIME ZONE")) {
            return DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE;
        } else if (type_name.startsWith("STRUCT")) {
            return DuckDBColumnType.STRUCT;
        } else if (type_name.startsWith("MAP")) {
            return DuckDBColumnType.MAP;
        } else if (type_name.startsWith("UNION")) {
            return DuckDBColumnType.UNION;
        }
        try {
            return DuckDBColumnType.valueOf(type_name);
        } catch (IllegalArgumentException e) {
            return DuckDBColumnType.UNKNOWN;
        }
    }

    protected int param_count;
    protected int column_count;
    protected String[] column_names;
    protected String[] column_types_string;
    protected String[] column_types_details;
    protected DuckDBColumnType[] column_types;
    protected DuckDBColumnTypeMetaData[] column_types_meta;
    protected final StatementReturnType return_type;

    public StatementReturnType getReturnType() {
        return return_type;
    }

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
        case LIST:
            return Types.ARRAY;
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
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
        case TIMESTAMP:
        case TIMESTAMP_NS:
            return Types.TIMESTAMP;
        case TIMESTAMP_WITH_TIME_ZONE:
            return Types.TIMESTAMP_WITH_TIMEZONE;
        case BLOB:
            return Types.BLOB;
        default:
            return Types.JAVA_OBJECT;
        }
    }

    public int getColumnType(int column) throws SQLException {
        if (column > column_count) {
            throw new SQLException("Column index out of bounds");
        }
        return type_to_int(column_types[column - 1]);
    }

    public String getColumnClassName(int column) throws SQLException {
        switch (column_types[column - 1]) {
        case BOOLEAN:
            return Boolean.class.getName();
        case TINYINT:
            return Byte.class.getName();
        case SMALLINT:
        case UTINYINT:
            return Short.class.getName();
        case INTEGER:
        case USMALLINT:
            return Integer.class.getName();
        case BIGINT:
        case UINTEGER:
            return Long.class.getName();
        case HUGEINT:
        case UBIGINT:
            return BigInteger.class.getName();
        case FLOAT:
            return Float.class.getName();
        case DOUBLE:
            return Double.class.getName();
        case DECIMAL:
            return BigDecimal.class.getName();
        case TIME:
            return LocalTime.class.getName();
        case TIME_WITH_TIME_ZONE:
            return OffsetTime.class.getName();
        case DATE:
            return LocalDate.class.getName();
        case TIMESTAMP:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
            return Timestamp.class.getName();
        case TIMESTAMP_WITH_TIME_ZONE:
            return OffsetDateTime.class.getName();
        case JSON:
            return JsonNode.class.getName();
        case BLOB:
            return DuckDBResultSet.DuckDBBlobResult.class.getName();
        case UUID:
            return UUID.class.getName();
        case LIST:
            return DuckDBArray.class.getName();
        case MAP:
            return HashMap.class.getName();
        case STRUCT:
            return DuckDBStruct.class.getName();
        default:
            return String.class.getName();
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
        return 0; // most systems will fall back to getPrecision
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

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private DuckDBColumnTypeMetaData typeMetadataForColumn(int columnIndex) throws SQLException {
        if (columnIndex > column_count) {
            throw new SQLException("Column index out of bounds");
        }
        return column_types_meta[columnIndex - 1];
    }
}
