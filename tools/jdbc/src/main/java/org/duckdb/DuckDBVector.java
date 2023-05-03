package org.duckdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.math.BigInteger;
import java.sql.SQLFeatureNotSupportedException;

public class DuckDBVector {
	
	public DuckDBVector(String duckdb_type, int length,  boolean[] nullmask) {
		super();
		this.duckdb_type = DuckDBResultSetMetaData.TypeNameToType(duckdb_type);
		this.length = length;
		this.nullmask = nullmask;
	}
	protected DuckDBColumnType duckdb_type;
	protected int length;
	protected boolean[] nullmask;
	protected ByteBuffer constlen_data = null;
	protected Object[] varlen_data = null;

	public Object getObject(int columnIndex) throws SQLException {
		switch (duckdb_type) {
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
		default:
			throw new SQLFeatureNotSupportedException(duckdb_type.toString());
		}
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
}
