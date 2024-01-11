package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.HashMap;
import java.util.Calendar;
import java.util.UUID;

class DuckDBVector {
    // Constant to construct BigDecimals from hugeint_t
    private final static BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");
    private final static DateTimeFormatter ERA_FORMAT =
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR_OF_ERA)
            .appendLiteral("-")
            .appendValue(ChronoField.MONTH_OF_YEAR)
            .appendLiteral("-")
            .appendValue(ChronoField.DAY_OF_MONTH)
            .appendOptional(new DateTimeFormatterBuilder()
                                .appendLiteral(" (")
                                .appendText(ChronoField.ERA, TextStyle.SHORT)
                                .appendLiteral(")")
                                .toFormatter())
            .toFormatter();

    DuckDBVector(String duckdb_type, int length, boolean[] nullmask) {
        super();
        this.duckdb_type = DuckDBResultSetMetaData.TypeNameToType(duckdb_type);
        this.meta = this.duckdb_type == DuckDBColumnType.DECIMAL
                        ? DuckDBColumnTypeMetaData.parseColumnTypeMetadata(duckdb_type)
                        : null;
        this.length = length;
        this.nullmask = nullmask;
    }
    private final DuckDBColumnTypeMetaData meta;
    protected final DuckDBColumnType duckdb_type;
    final int length;
    private final boolean[] nullmask;
    private ByteBuffer constlen_data = null;
    private Object[] varlen_data = null;

    Object getObject(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        switch (duckdb_type) {
        case BOOLEAN:
            return getBoolean(idx);
        case TINYINT:
            return getByte(idx);
        case SMALLINT:
            return getShort(idx);
        case INTEGER:
            return getInt(idx);
        case BIGINT:
            return getLong(idx);
        case HUGEINT:
            return getHugeint(idx);
        case UTINYINT:
            return getUint8(idx);
        case USMALLINT:
            return getUint16(idx);
        case UINTEGER:
            return getUint32(idx);
        case UBIGINT:
            return getUint64(idx);
        case FLOAT:
            return getFloat(idx);
        case DOUBLE:
            return getDouble(idx);
        case DECIMAL:
            return getBigDecimal(idx);
        case TIME:
            return getLocalTime(idx);
        case TIME_WITH_TIME_ZONE:
            return getOffsetTime(idx);
        case DATE:
            return getLocalDate(idx);
        case TIMESTAMP:
        case TIMESTAMP_NS:
        case TIMESTAMP_S:
        case TIMESTAMP_MS:
            return getTimestamp(idx);
        case TIMESTAMP_WITH_TIME_ZONE:
            return getOffsetDateTime(idx);
        case JSON:
            return getJsonObject(idx);
        case BLOB:
            return getBlob(idx);
        case UUID:
            return getUuid(idx);
        case MAP:
            return getMap(idx);
        case LIST:
            return getArray(idx);
        case STRUCT:
            return getStruct(idx);
        case UNION:
            return getUnion(idx);
        default:
            return getLazyString(idx);
        }
    }

    LocalTime getLocalTime(int idx) throws SQLException {
        String lazyString = getLazyString(idx);

        return lazyString == null ? null : LocalTime.parse(lazyString);
    }

    LocalDate getLocalDate(int idx) throws SQLException {
        String lazyString = getLazyString(idx);

        if ("infinity".equals(lazyString))
            return LocalDate.MAX;
        else if ("-infinity".equals(lazyString))
            return LocalDate.MIN;

        return lazyString == null ? null : LocalDate.from(ERA_FORMAT.parse(lazyString));
    }

    BigDecimal getBigDecimal(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.DECIMAL)) {
            switch (meta.type_size) {
            case 16:
                return new BigDecimal((int) getbuf(idx, 2).getShort()).scaleByPowerOfTen(meta.scale * -1);
            case 32:
                return new BigDecimal(getbuf(idx, 4).getInt()).scaleByPowerOfTen(meta.scale * -1);
            case 64:
                return new BigDecimal(getbuf(idx, 8).getLong()).scaleByPowerOfTen(meta.scale * -1);
            case 128:
                ByteBuffer buf = getbuf(idx, 16);
                long lower = buf.getLong();
                long upper = buf.getLong();
                return new BigDecimal(upper)
                    .multiply(ULONG_MULTIPLIER)
                    .add(new BigDecimal(Long.toUnsignedString(lower)))
                    .scaleByPowerOfTen(meta.scale * -1);
            }
        }
        Object o = getObject(idx);
        return new BigDecimal(o.toString());
    }

    OffsetDateTime getOffsetDateTime(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        if (isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return DuckDBTimestamp.toOffsetDateTime(getbuf(idx, 8).getLong());
        }
        Object o = getObject(idx);
        return OffsetDateTime.parse(o.toString());
    }

    Timestamp getTimestamp(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        if (isType(DuckDBColumnType.TIMESTAMP) || isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return DuckDBTimestamp.toSqlTimestamp(getbuf(idx, 8).getLong());
        }
        if (isType(DuckDBColumnType.TIMESTAMP_MS)) {
            return DuckDBTimestamp.toSqlTimestamp(getbuf(idx, 8).getLong() * 1000);
        }
        if (isType(DuckDBColumnType.TIMESTAMP_NS)) {
            return DuckDBTimestamp.toSqlTimestampNanos(getbuf(idx, 8).getLong());
        }
        if (isType(DuckDBColumnType.TIMESTAMP_S)) {
            return DuckDBTimestamp.toSqlTimestamp(getbuf(idx, 8).getLong() * 1_000_000);
        }
        Object o = getObject(idx);
        return Timestamp.valueOf(o.toString());
    }

    UUID getUuid(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }

        if (isType(DuckDBColumnType.UUID)) {
            ByteBuffer buffer = getbuf(idx, 16);
            long leastSignificantBits = buffer.getLong();

            // Account for unsigned
            long mostSignificantBits = buffer.getLong() - Long.MAX_VALUE - 1;
            return new UUID(mostSignificantBits, leastSignificantBits);
        }
        Object o = getObject(idx);
        return UUID.fromString(o.toString());
    }

    String getLazyString(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        return varlen_data[idx].toString();
    }

    Array getArray(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.LIST)) {
            return (Array) varlen_data[idx];
        }
        throw new SQLFeatureNotSupportedException("getArray");
    }

    Map<Object, Object> getMap(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (!isType(DuckDBColumnType.MAP)) {
            throw new SQLFeatureNotSupportedException("getMap");
        }

        Object[] entries = (Object[]) (((Array) varlen_data[idx]).getArray());
        Map<Object, Object> result = new HashMap<>();

        for (Object entry : entries) {
            Object[] entry_val = ((Struct) entry).getAttributes();
            result.put(entry_val[0], entry_val[1]);
        }

        return result;
    }

    Blob getBlob(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.BLOB)) {
            return new DuckDBResultSet.DuckDBBlobResult((ByteBuffer) varlen_data[idx]);
        }

        throw new SQLFeatureNotSupportedException("getBlob");
    }

    JsonNode getJsonObject(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        String result = getLazyString(idx);
        return result == null ? null : new JsonNode(result);
    }

    Date getDate(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        // TODO: load from native format
        String string_value = getLazyString(idx);
        if (string_value == null) {
            return null;
        }
        try {
            return Date.valueOf(string_value);
        } catch (Exception e) {
            return null;
        }
    }

    OffsetTime getOffsetTime(int idx) {
        if (check_and_null(idx)) {
            return null;
        }
        return DuckDBTimestamp.toOffsetTime(getbuf(idx, 8).getLong());
    }

    Time getTime(int idx) {
        // TODO: load from native format
        String string_value = getLazyString(idx);
        if (string_value == null) {
            return null;
        }
        try {
            return Time.valueOf(string_value);
        } catch (Exception e) {
            return null;
        }
    }

    Boolean getBoolean(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return false;
        }
        if (isType(DuckDBColumnType.BOOLEAN)) {
            return getbuf(idx, 1).get() == 1;
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).byteValue() == 1;
        }

        return Boolean.parseBoolean(o.toString());
    }

    protected ByteBuffer getbuf(int idx, int typeWidth) {
        ByteBuffer buf = constlen_data;
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.position(idx * typeWidth);
        return buf;
    }

    protected boolean check_and_null(int idx) {
        return nullmask[idx];
    }

    long getLong(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.BIGINT) || isType(DuckDBColumnType.TIMESTAMP) ||
            isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return getbuf(idx, 8).getLong();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        return Long.parseLong(o.toString());
    }

    int getInt(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.INTEGER)) {
            return getbuf(idx, 4).getInt();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).intValue();
        }
        return Integer.parseInt(o.toString());
    }

    short getUint8(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.UTINYINT)) {
            ByteBuffer buf = ByteBuffer.allocate(2);
            getbuf(idx, 1).get(buf.array(), 1, 1);
            return buf.getShort();
        }
        throw new SQLFeatureNotSupportedException("getUint8");
    }

    long getUint32(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.UINTEGER)) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            getbuf(idx, 4).get(buf.array(), 0, 4);
            return buf.getLong();
        }
        throw new SQLFeatureNotSupportedException("getUint32");
    }

    int getUint16(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.USMALLINT)) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            getbuf(idx, 2).get(buf.array(), 0, 2);
            return buf.getInt();
        }
        throw new SQLFeatureNotSupportedException("getUint16");
    }

    BigInteger getUint64(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return BigInteger.ZERO;
        }
        if (isType(DuckDBColumnType.UBIGINT)) {
            byte[] buf_res = new byte[16];
            byte[] buf = new byte[8];
            getbuf(idx, 8).get(buf);
            for (int i = 0; i < 8; i++) {
                buf_res[i + 8] = buf[7 - i];
            }
            return new BigInteger(buf_res);
        }
        throw new SQLFeatureNotSupportedException("getUint64");
    }

    double getDouble(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return Double.NaN;
        }
        if (isType(DuckDBColumnType.DOUBLE)) {
            return getbuf(idx, 8).getDouble();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        return Double.parseDouble(o.toString());
    }

    byte getByte(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.TINYINT)) {
            return getbuf(idx, 1).get();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).byteValue();
        }
        return Byte.parseByte(o.toString());
    }

    short getShort(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return 0;
        }
        if (isType(DuckDBColumnType.SMALLINT)) {
            return getbuf(idx, 2).getShort();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).shortValue();
        }
        return Short.parseShort(o.toString());
    }

    BigInteger getHugeint(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return BigInteger.ZERO;
        }
        if (isType(DuckDBColumnType.HUGEINT)) {
            byte[] buf = new byte[16];
            getbuf(idx, 16).get(buf);
            for (int i = 0; i < 8; i++) {
                byte keep = buf[i];
                buf[i] = buf[15 - i];
                buf[15 - i] = keep;
            }
            return new BigInteger(buf);
        }
        Object o = getObject(idx);
        return new BigInteger(o.toString());
    }

    float getFloat(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return Float.NaN;
        }
        if (isType(DuckDBColumnType.FLOAT)) {
            return getbuf(idx, 4).getFloat();
        }
        Object o = getObject(idx);
        if (o instanceof Number) {
            return ((Number) o).floatValue();
        }
        return Float.parseFloat(o.toString());
    }

    private boolean isType(DuckDBColumnType columnType) {
        return duckdb_type == columnType;
    }

    Timestamp getTimestamp(int idx, Calendar cal) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        // Our raw data is already a proper count of units since the epoch
        // So just construct the SQL Timestamp.
        if (isType(DuckDBColumnType.TIMESTAMP) || isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return DuckDBTimestamp.fromMicroInstant(getbuf(idx, 8).getLong());
        }
        if (isType(DuckDBColumnType.TIMESTAMP_MS)) {
            return DuckDBTimestamp.fromMilliInstant(getbuf(idx, 8).getLong());
        }
        if (isType(DuckDBColumnType.TIMESTAMP_NS)) {
            return DuckDBTimestamp.fromNanoInstant(getbuf(idx, 8).getLong());
        }
        if (isType(DuckDBColumnType.TIMESTAMP_S)) {
            return DuckDBTimestamp.fromSecondInstant(getbuf(idx, 8).getLong());
        }
        Object o = getObject(idx);
        return Timestamp.valueOf(o.toString());
    }

    LocalDateTime getLocalDateTime(int idx) throws SQLException {
        if (check_and_null(idx)) {
            return null;
        }
        if (isType(DuckDBColumnType.TIMESTAMP) || isType(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
            return DuckDBTimestamp.toLocalDateTime(getbuf(idx, 8).getLong());
        }
        Object o = getObject(idx);
        return LocalDateTime.parse(o.toString());
    }

    Struct getStruct(int idx) {
        return check_and_null(idx) ? null : (Struct) varlen_data[idx];
    }

    Object getUnion(int idx) throws SQLException {
        if (check_and_null(idx))
            return null;

        Struct struct = getStruct(idx);

        Object[] attributes = struct.getAttributes();

        short tag = (short) attributes[0];

        return attributes[1 + tag];
    }
}
