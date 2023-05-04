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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Calendar;
import java.util.UUID;

public class DuckDBVector {
	// Constant to construct BigDecimals from hugeint_t
	private final static BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");

	private final DuckDBColumnTypeMetaData meta;

	public DuckDBVector(String duckdb_type, int length,  boolean[] nullmask) {
		super();
		this.duckdb_type = DuckDBResultSetMetaData.TypeNameToType(duckdb_type);
		this.meta = this.duckdb_type == DuckDBColumnType.DECIMAL ? DuckDBColumnTypeMetaData.parseColumnTypeMetadata(duckdb_type) : null;
		this.length = length;
		this.nullmask = nullmask;
	}
	protected DuckDBColumnType duckdb_type;
	protected int length;
	protected boolean[] nullmask;
	protected ByteBuffer constlen_data = null;
	protected Object[] varlen_data = null;

	public Object getObject(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		switch (duckdb_type) {
			case BOOLEAN:
				return getBoolean(columnIndex);
			case TINYINT:
				return getByte(columnIndex);
			case SMALLINT:
				return getShort(columnIndex);
			case INTEGER:
				return getInt(columnIndex);
			case BIGINT:
				return getLong(columnIndex);
			case HUGEINT:
				return getHugeint(columnIndex);
			case UTINYINT:
				return getUint8(columnIndex);
			case USMALLINT:
				return getUint16(columnIndex);
			case UINTEGER:
				return getUint32(columnIndex);
			case UBIGINT:
				return getUint64(columnIndex);
			case FLOAT:
				return getFloat(columnIndex);
			case DOUBLE:
				return getDouble(columnIndex);
			case DECIMAL:
				return getBigDecimal(columnIndex);
			case TIME:
				return getTime(columnIndex);
			case TIME_WITH_TIME_ZONE:
				return getOffsetTime(columnIndex);
			case DATE:
				return getDate(columnIndex);
			case TIMESTAMP:
			case TIMESTAMP_NS:
			case TIMESTAMP_S:
			case TIMESTAMP_MS:
				return getTimestamp(columnIndex);
			case TIMESTAMP_WITH_TIME_ZONE:
				return getOffsetDateTime(columnIndex);
			case JSON:
				return getJsonObject(columnIndex);
			case BLOB:
				return getBlob(columnIndex);
			case UUID:
				return getUuid(columnIndex);
			case LIST:
				return getArray(columnIndex);
			default:
				return getLazyString(columnIndex);
		}
	}

	BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.DECIMAL)) {
			switch (meta.type_size) {
				case 16:
					return new BigDecimal((int) getbuf(columnIndex, 2).getShort())
							.scaleByPowerOfTen(meta.scale * -1);
				case 32:
					return new BigDecimal(getbuf(columnIndex, 4).getInt())
							.scaleByPowerOfTen(meta.scale * -1);
				case 64:
					return new BigDecimal(getbuf(columnIndex, 8).getLong())
							.scaleByPowerOfTen(meta.scale * -1);
				case 128:
					ByteBuffer buf = getbuf(columnIndex, 16);
					long lower = buf.getLong();
					long upper = buf.getLong();
					return new BigDecimal(upper).multiply(ULONG_MULTIPLIER)
							.add(new BigDecimal(Long.toUnsignedString(lower)))
							.scaleByPowerOfTen(meta.scale * -1);
			}
		}
		Object o = getObject(columnIndex);
		return new BigDecimal(o.toString());
	}

	OffsetDateTime getOffsetDateTime(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}

		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
			return DuckDBTimestamp.toOffsetDateTime(getbuf(columnIndex, 8).getLong());
		}
		Object o = getObject(columnIndex);
		return OffsetDateTime.parse(o.toString());
	}

	Timestamp getTimestamp(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}

		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return DuckDBTimestamp.toSqlTimestamp(getbuf(columnIndex, 8).getLong());
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_MS)) {
			return DuckDBTimestamp.toSqlTimestamp(getbuf(columnIndex, 8).getLong() * 1000);
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_NS)) {
			return DuckDBTimestamp.toSqlTimestampNanos(getbuf(columnIndex, 8).getLong());
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_S)) {
			return DuckDBTimestamp.toSqlTimestamp(getbuf(columnIndex, 8).getLong() * 1_000_000);
		}
		Object o = getObject(columnIndex);
		return Timestamp.valueOf(o.toString());
	}

	UUID getUuid(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}

		if (isType(columnIndex, DuckDBColumnType.UUID)) {
			ByteBuffer buffer = getbuf(columnIndex, 16);
			long leastSignificantBits = buffer.getLong();

			// Account for unsigned
			long mostSignificantBits = buffer.getLong() - Long.MAX_VALUE - 1;
			return new UUID(mostSignificantBits, leastSignificantBits);
		}
		Object o = getObject(columnIndex);
		return UUID.fromString(o.toString());
	}

	public String getLazyString(int idx) {
		if (check_and_null(idx)) {
			return null;
		}
		return (String) varlen_data[idx];
	}

	public Array getArray(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.LIST)) {
			return (Array) varlen_data[columnIndex];
		}
		throw new SQLFeatureNotSupportedException("getArray");
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.BLOB)) {
			return new DuckDBResultSet.DuckDBBlobResult((ByteBuffer) varlen_data[columnIndex]);
		}

		throw new SQLFeatureNotSupportedException("getBlob");
	}

	public JsonNode getJsonObject(int idx) {
		if (check_and_null(idx)) {
			return null;
		}
		String result = getLazyString(idx);
		return result == null ? null : new JsonNode(result);
	}

	public Date getDate(int idx) {
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

	public OffsetTime getOffsetTime(int columnIndex) {
		if (check_and_null(columnIndex)) {
			return null;
		}
		return DuckDBTimestamp.toOffsetTime(getbuf(columnIndex, 8).getLong());
	}

	public Time getTime(int idx) {
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

	public Boolean getBoolean(int idx) throws SQLException {
		if (check_and_null(idx)) {
			return false;
		}
		if (isType(idx, DuckDBColumnType.BOOLEAN)) {
			return getbuf(idx, 1).get() == 1;
		}
		Object o = getObject(idx);
		if (o instanceof Number) {
			return ((Number) o).byteValue() == 1;
		}

		return Boolean.parseBoolean(o.toString());
	}

	protected ByteBuffer getbuf(int columnIndex, int typeWidth) {
		ByteBuffer buf = constlen_data;
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.position(columnIndex * typeWidth);
		return buf;
	}

	private boolean check_and_null(int columnIndex) {
		return nullmask[columnIndex];
	}

	public long getLong(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.BIGINT) || isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return getbuf(columnIndex, 8).getLong();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).longValue();
		}
		return Long.parseLong(o.toString());
	}

	public int getInt(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.INTEGER)) {
			return getbuf(columnIndex, 4).getInt();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).intValue();
		}
		return Integer.parseInt(o.toString());
	}

	public short getUint8(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.UTINYINT)) {
			ByteBuffer buf = ByteBuffer.allocate(2);
			getbuf(columnIndex, 1).get(buf.array(), 1, 1);
			return buf.getShort();
		}
		throw new SQLFeatureNotSupportedException("getUint8");
	}

	public long getUint32(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.UINTEGER)) {
			ByteBuffer buf = ByteBuffer.allocate(8);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			getbuf(columnIndex, 4).get(buf.array(), 0, 4);
			return buf.getLong();
		}
		throw new SQLFeatureNotSupportedException("getUint32");
	}

	public int getUint16(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.USMALLINT)) {
			ByteBuffer buf = ByteBuffer.allocate(4);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			getbuf(columnIndex, 2).get(buf.array(), 0, 2);
			return buf.getInt();
		}
		throw new SQLFeatureNotSupportedException("getUint16");
	}

	public BigInteger getUint64(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return BigInteger.ZERO;
		}
		if (isType(columnIndex, DuckDBColumnType.UBIGINT)) {
			byte[] buf_res = new byte[16];
			byte[] buf = new byte[8];
			getbuf(columnIndex, 8).get(buf);
			for (int i = 0; i < 8; i++) {
				buf_res[i + 8] = buf[7 - i];
			}
			return new BigInteger(buf_res);
		}
		throw new SQLFeatureNotSupportedException("getUint64");
	}

	public double getDouble(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return Double.NaN;
		}
		if (isType(columnIndex, DuckDBColumnType.DOUBLE)) {
			return getbuf(columnIndex, 8).getDouble();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).doubleValue();
		}
		return Double.parseDouble(o.toString());
	}

	public byte getByte(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.TINYINT)) {
			return getbuf(columnIndex, 1).get();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).byteValue();
		}
		return Byte.parseByte(o.toString());
	}

	public short getShort(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.SMALLINT)) {
			return getbuf(columnIndex, 2).getShort();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).shortValue();
		}
		return Short.parseShort(o.toString());
	}

	public BigInteger getHugeint(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return BigInteger.ZERO;
		}
		if (isType(columnIndex, DuckDBColumnType.HUGEINT)) {
			byte[] buf = new byte[16];
			getbuf(columnIndex, 16).get(buf);
			for (int i = 0; i < 8; i++) {
				byte keep = buf[i];
				buf[i] = buf[15 - i];
				buf[15 - i] = keep;
			}
			return new BigInteger(buf);
		}
		Object o = getObject(columnIndex);
		return new BigInteger(o.toString());
	}

	public float getFloat(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return Float.NaN;
		}
		if (isType(columnIndex, DuckDBColumnType.FLOAT)) {
			return getbuf(columnIndex, 4).getFloat();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).floatValue();
		}
		return Float.parseFloat(o.toString());
	}

	private boolean isType(int columnIndex, DuckDBColumnType columnType) {
		return duckdb_type == columnType;
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		// Our raw data is already a proper count of units since the epoch
		// So just construct the SQL Timestamp.
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return DuckDBTimestamp.fromMicroInstant(getbuf(columnIndex, 8).getLong(columnIndex));
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_MS)) {
			return DuckDBTimestamp.fromMilliInstant(getbuf(columnIndex, 8).getLong());
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_NS)) {
			return DuckDBTimestamp.fromNanoInstant(getbuf(columnIndex, 8).getLong());
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_S)) {
			return DuckDBTimestamp.fromSecondInstant(getbuf(columnIndex, 8).getLong());
		}
		Object o = getObject(columnIndex);
		return Timestamp.valueOf(o.toString());
	}

	public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return DuckDBTimestamp.toLocalDateTime(getbuf(columnIndex, 8).getLong());
		}
		Object o = getObject(columnIndex);
		return LocalDateTime.parse(o.toString());
	}
}
