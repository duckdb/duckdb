package org.duckdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class DuckDBResultSet implements ResultSet {

	// Constant to construct BigDecimals from hugeint_t 
	private final static BigDecimal ULONG_MULTIPLIER = new BigDecimal("18446744073709551616");

	private DuckDBPreparedStatement stmt;
	private DuckDBResultSetMetaData meta;

	private ByteBuffer result_ref;
	private DuckDBVector[] current_chunk;
	private int chunk_idx = 0;
	private boolean finished = false;
	private boolean was_null;

	public DuckDBResultSet(DuckDBPreparedStatement stmt, DuckDBResultSetMetaData meta, ByteBuffer result_ref) throws SQLException {
		this.stmt = stmt;
		this.result_ref = result_ref;
		this.meta = meta;
		current_chunk = DuckDBNative.duckdb_jdbc_fetch(result_ref);
		if (current_chunk.length == 0) {
			finished = true;
		}
	}

	public Statement getStatement() throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		return stmt;
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		return meta;
	}

	public boolean next() throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		if (finished) {
			return false;
		}
		chunk_idx++;
		if (chunk_idx > current_chunk[0].length) {
			current_chunk = DuckDBNative.duckdb_jdbc_fetch(result_ref);
			chunk_idx = 1;
		}
		if (current_chunk.length == 0) {
			finished = true;
			return false;
		}
		return true;
	}

	public synchronized void close() throws SQLException {
		if (result_ref != null) {
			DuckDBNative.duckdb_jdbc_free_result(result_ref);
			result_ref = null;
		}
		stmt = null;
		meta = null;
		current_chunk = null;
	}

	protected void finalize() throws Throwable {
		close();
	}

	public boolean isClosed() throws SQLException {
		return result_ref == null;
	}

	private void check(int columnIndex) throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		if (columnIndex < 1 || columnIndex > meta.column_count) {
			throw new SQLException("Column index out of bounds");
		}

	}

	public Object getObject(int columnIndex) throws SQLException {
		check_and_null(columnIndex);
		if (was_null) {
			return null;
		}
		switch (meta.column_types[columnIndex - 1]) {
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
		case VARCHAR:
			return getString(columnIndex);
		case ENUM:
			return getString(columnIndex);
		case TIME:
			return getTime(columnIndex);
		case DATE:
			return getDate(columnIndex);
		case TIMESTAMP:
			return getTimestamp(columnIndex);
		case TIMESTAMP_WITH_TIME_ZONE:
			return getOffsetDateTime(columnIndex);
		case INTERVAL:
			return getLazyString(columnIndex);
		default:
			throw new SQLException("Not implemented type: " + meta.column_types_string[columnIndex - 1]);
		}

	}

	public boolean wasNull() throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		return was_null;
	}

	private boolean check_and_null(int columnIndex) throws SQLException {
		check(columnIndex);
		was_null = current_chunk[columnIndex - 1].nullmask[chunk_idx - 1];
		return was_null;
	}

	public String getLazyString(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		return (String) current_chunk[columnIndex - 1].varlen_data[chunk_idx - 1];
	}

	private boolean isType(int columnIndex, DuckDBColumnType type) {
		return meta.column_types[columnIndex - 1] == type;
	}

	public String getString(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}

		if (isType(columnIndex, DuckDBColumnType.VARCHAR)
			|| isType(columnIndex, DuckDBColumnType.ENUM)) {
			return (String) current_chunk[columnIndex - 1].varlen_data[chunk_idx - 1];
		}
		Object res = getObject(columnIndex);
		if (res == null) {
			return null;
		} else {
			return res.toString();
		}
	}

	private ByteBuffer getbuf(int columnIndex, int typeWidth) throws SQLException {
		ByteBuffer buf = current_chunk[columnIndex - 1].constlen_data;
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.position((chunk_idx - 1) * typeWidth);
		return buf;
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return false;
		}
		if (isType(columnIndex, DuckDBColumnType.BOOLEAN)) {
			return getbuf(columnIndex, 1).get() == 1;
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).byteValue() == 1;
		}

		return Boolean.parseBoolean(getObject(columnIndex).toString());
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

	private short getUint8(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.UTINYINT)) {
			ByteBuffer buf = ByteBuffer.allocate(2);
			getbuf(columnIndex, 1).get(buf.array(), 1, 1);
			return buf.getShort();

		}
		throw new SQLFeatureNotSupportedException();
	}

	private int getUint16(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.USMALLINT)) {
			ByteBuffer buf = ByteBuffer.allocate(4);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			getbuf(columnIndex, 2).get(buf.array(), 0, 2);
			return buf.getInt();
		}
		throw new SQLFeatureNotSupportedException();

	}

	private long getUint32(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.UINTEGER)) {
			ByteBuffer buf = ByteBuffer.allocate(8);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			getbuf(columnIndex, 4).get(buf.array(), 0, 4);
			return buf.getLong();
		}
		throw new SQLFeatureNotSupportedException();
	}

	private BigInteger getUint64(int columnIndex) throws SQLException {
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
		throw new SQLFeatureNotSupportedException();
	}

	public long getLong(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if (isType(columnIndex, DuckDBColumnType.BIGINT)) {
			return getbuf(columnIndex, 8).getLong();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).longValue();
		}
		return Long.parseLong(o.toString());
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

	public int findColumn(String columnLabel) throws SQLException {
		if (isClosed()) {
			throw new SQLException("ResultSet was closed");
		}
		for (int col_idx = 0; col_idx < meta.column_count; col_idx++) {
			if (meta.column_names[col_idx].contentEquals(columnLabel)) {
				return col_idx + 1;
			}
		}
		throw new SQLException("Could not find column with label " + columnLabel);
	}

	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(int columnIndex) throws SQLException {
		String string_value = getLazyString(columnIndex);
		if (string_value == null) {
			return null;
		}
		try {
			return Date.valueOf(string_value);
		} catch (Exception e) {
			return null;
		}
	}

	public Time getTime(int columnIndex) throws SQLException {
		String string_value = getLazyString(columnIndex);
		if (string_value == null) {
			return null;
		}
		try {

			return Time.valueOf(getLazyString(columnIndex));
		} catch (Exception e) {
			return null;
		}
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return DuckDBTimestamp.toSqlTimestamp(getbuf(columnIndex, 8).getLong());
		}
		return null;
	}

	private LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP)) {
			return DuckDBTimestamp.toLocalDateTime(getbuf(columnIndex, 8).getLong());
		}
		return null;
	}

	private OffsetDateTime getOffsetDateTime(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)) {
			return DuckDBTimestamp.toOffsetDateTime(getbuf(columnIndex, 8).getLong());
		}
		return null;
	}

	static class DuckDBBlobResult implements Blob {

		static class ByteBufferBackedInputStream extends InputStream {

			ByteBuffer buf;

			public ByteBufferBackedInputStream(ByteBuffer buf) {
				this.buf = buf;
			}

			public int read() throws IOException {
				if (!buf.hasRemaining()) {
					return -1;
				}
				return buf.get() & 0xFF;
			}

			public int read(byte[] bytes, int off, int len) throws IOException {
				if (!buf.hasRemaining()) {
					return -1;
				}

				len = Math.min(len, buf.remaining());
				buf.get(bytes, off, len);
				return len;
			}
		}

		public DuckDBBlobResult(ByteBuffer buffer_p) {
			buffer_p.position(0);
			buffer_p.order(ByteOrder.LITTLE_ENDIAN);
			this.buffer = buffer_p;
		}

		public InputStream getBinaryStream() {
			return getBinaryStream(0, length());
		}

		public InputStream getBinaryStream(long pos, long length) {
			return new ByteBufferBackedInputStream(buffer);
		}

		public byte[] getBytes(long pos, int length) {
			byte[] bytes = new byte[length];
			buffer.position((int) pos);
			buffer.get(bytes, 0, length);
			return bytes;
		}

		public long position(Blob pattern, long start) throws SQLException {
			throw new SQLFeatureNotSupportedException();
		}

		public long position(byte[] pattern, long start) throws SQLException {
			throw new SQLFeatureNotSupportedException();
		}

		public long length() {
			return buffer.capacity();
		}

		public void free() {
			// nop
		}

		public OutputStream setBinaryStream(long pos) throws SQLException {
			throw new SQLFeatureNotSupportedException();
		}

		public void truncate(long length) throws SQLException {
			throw new SQLFeatureNotSupportedException();
		}

		public int setBytes(long pos, byte[] bytes) throws SQLException {
			throw new SQLFeatureNotSupportedException();

		}

		public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
			throw new SQLFeatureNotSupportedException();

		}

		private ByteBuffer buffer;

	}

	public Blob getBlob(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.BLOB)) {
			return new DuckDBBlobResult(((ByteBuffer[]) current_chunk[columnIndex - 1].varlen_data)[chunk_idx - 1]);
		}

		throw new SQLFeatureNotSupportedException();
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(findColumn(columnLabel));
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if (isType(columnIndex, DuckDBColumnType.DECIMAL)) {
			switch(meta.column_types_meta[columnIndex -1].type_size){
			case 16:
				return new BigDecimal((int)getbuf(columnIndex, 2).getShort())
					.scaleByPowerOfTen(meta.column_types_meta[columnIndex -1].scale * -1);
			case 32:
				return new BigDecimal(getbuf(columnIndex, 4).getInt())
					.scaleByPowerOfTen(meta.column_types_meta[columnIndex -1].scale * -1);
			case 64:
				return new BigDecimal(getbuf(columnIndex, 8).getLong())
					.scaleByPowerOfTen(meta.column_types_meta[columnIndex -1].scale * -1);
			case 128:
				ByteBuffer buf = getbuf(columnIndex, 16);
				long lower = buf.getLong(); 
				long upper = buf.getLong();
				return new BigDecimal(upper).multiply(ULONG_MULTIPLIER).add(new BigDecimal(Long.toUnsignedString(lower)))
					.scaleByPowerOfTen(meta.column_types_meta[columnIndex -1].scale * -1);
			}
		}
		Object o = getObject(columnIndex);
		return new BigDecimal(o.toString());
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	public boolean isBeforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isAfterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean first() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean last() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean absolute(int row) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean relative(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean previous() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_UNKNOWN) {
			throw new SQLFeatureNotSupportedException();
		}
	}

	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	public void setFetchSize(int rows) throws SQLException {
		if (rows < 0) {
			throw new SQLException("Fetch size has to be >= 0");
		}
		// whatevs
	}

	public int getFetchSize() throws SQLException {
		return DuckDBNative.duckdb_jdbc_fetch_size();
	}

	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	public boolean rowUpdated() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean rowInserted() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean rowDeleted() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getNString(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type == null) {
			throw new SQLException("type is null");
		}

		DuckDBColumnType sqlType = meta.column_types[columnIndex - 1]; 
		// Missing: unsigned types like UINTEGER, more liberal casting, e.g. SMALLINT -> Integer
		// Compare results with expected results from Javadoc
		// https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/ResultSet.html
		if (type == BigDecimal.class) {
			if (sqlType == DuckDBColumnType.DECIMAL) {
				return type.cast(getBigDecimal(columnIndex));
			} else {
				throw new SQLException("Can't convert value to BigDecimal " + type.toString());
			}
		} else if (type == String.class) {
			if (sqlType == DuckDBColumnType.VARCHAR
					|| sqlType == DuckDBColumnType.ENUM) {
				return type.cast(getString(columnIndex));
			} else {
				throw new SQLException("Can't convert value to String " + type.toString());
			}
		} else if (type == Boolean.class) {
			if (sqlType == DuckDBColumnType.BOOLEAN) { 
				return type.cast(getBoolean(columnIndex));
			} else {
				throw new SQLException("Can't convert value to boolean " + type.toString());
			}
		} else if (type == Short.class) {
			if (sqlType == DuckDBColumnType.SMALLINT) { 
				return type.cast(getShort(columnIndex));
			} else {
				throw new SQLException("Can't convert value to short " + type.toString());
			}
		} else if (type == Integer.class) {
			if (sqlType == DuckDBColumnType.INTEGER) { 
				return type.cast(getInt(columnIndex));
			} else if (sqlType == DuckDBColumnType.SMALLINT){
				return type.cast(getShort(columnIndex));
			} else if (sqlType == DuckDBColumnType.TINYINT){
				return type.cast(getByte(columnIndex));
			} else if (sqlType == DuckDBColumnType.USMALLINT){
				throw new SQLException("Can't convert value to integer " + type.toString());
			// return type.cast(getShort(columnIndex));
			} else if (sqlType == DuckDBColumnType.UTINYINT){
				throw new SQLException("Can't convert value to integer " + type.toString());
			// return type.cast(getShort(columnIndex));
			} else {
				throw new SQLException("Can't convert value to integer " + type.toString());
			}
		} else if (type == Long.class) {
			if (sqlType == DuckDBColumnType.BIGINT) { 
				return type.cast(getLong(columnIndex));
			} else if (sqlType == DuckDBColumnType.UINTEGER) { 
				throw new SQLException("Can't convert value to long " + type.toString());
			// return type.cast(getLong(columnIndex));
			} else {
				throw new SQLException("Can't convert value to long " + type.toString());
			}
		} else if (type == Float.class) {
			if (sqlType == DuckDBColumnType.FLOAT) { 
				return type.cast(getFloat(columnIndex));
			} else {
				throw new SQLException("Can't convert value to float " + type.toString());
			}
		} else if (type == Double.class) {
			if (sqlType == DuckDBColumnType.DOUBLE) { 
				return type.cast(getDouble(columnIndex));
			} else {
				throw new SQLException("Can't convert value to float " + type.toString());
			}
		} else if (type == Date.class) {
			if (sqlType == DuckDBColumnType.DATE) { 
				return type.cast(getDate(columnIndex));
			} else {
				throw new SQLException("Can't convert value to Date " + type.toString());
			}
		} else if (type == Time.class) {
			if (sqlType == DuckDBColumnType.TIME) { 
				return type.cast(getTime(columnIndex));
			} else {
				throw new SQLException("Can't convert value to Time " + type.toString());
			}
		} else if (type == Timestamp.class) {
			if (sqlType == DuckDBColumnType.TIMESTAMP) { 
				return type.cast(getTimestamp(columnIndex));
			} else {
				throw new SQLException("Can't convert value to Timestamp " + type.toString());
			}
		} else if (type == LocalDateTime.class) {
			if (sqlType == DuckDBColumnType.TIMESTAMP) {
				return type.cast(getLocalDateTime(columnIndex));
			} else {
				throw new SQLException("Can't convert value to LocalDateTime " + type.toString());
			}
		} else if (type == BigInteger.class) {
			if (sqlType == DuckDBColumnType.HUGEINT) {
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
			// return type.cast(getLocalDateTime(columnIndex));
			} else if (sqlType == DuckDBColumnType.UBIGINT) {
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
			// return type.cast(getLocalDateTime(columnIndex));
			} else {
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
			}
		} else if (type == OffsetDateTime.class) {
			if (sqlType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE) {
				return type.cast(getOffsetDateTime(columnIndex));
			} else {
				throw new SQLException("Can't convert value to OffsetDateTime " + type.toString());
			}
		} else if (type == Blob.class) {
			if (sqlType == DuckDBColumnType.BLOB) {
				throw new SQLException("Can't convert value to Blob " + type.toString());
			// return type.cast(getLocalDateTime(columnIndex));
			} else {
				throw new SQLException("Can't convert value to Blob " + type.toString());
			}
		} else {
			throw new SQLException("Can't convert value to " + type +  " " +  type.toString());
		}
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
