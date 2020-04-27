package nl.cwi.da.duckdb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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

public class DuckDBResultSet implements ResultSet {

	private DuckDBPreparedStatement stmt;
	private DuckDBResultSetMetaData meta;

	private ByteBuffer result_ref;
	private DuckDBVector[] current_chunk;
	private int chunk_idx = 0;
	private boolean finished = false;
	private boolean was_null;

	public DuckDBResultSet(DuckDBPreparedStatement stmt, DuckDBResultSetMetaData meta, ByteBuffer result_ref) {
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
		case "BOOLEAN":
			return getBoolean(columnIndex);
		case "TINYINT":
			return getByte(columnIndex);
		case "SMALLINT":
			return getShort(columnIndex);
		case "INTEGER":
			return getInt(columnIndex);
		case "BIGINT":
			return getLong(columnIndex);
		case "FLOAT":
			return getFloat(columnIndex);
		case "DOUBLE":
			return getDouble(columnIndex);
		case "VARCHAR":
			return getString(columnIndex);
		default:
			throw new SQLException("Not implemented type: " + meta.column_types[columnIndex - 1]);
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

	public String getString(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return null;
		}
		if ("VARCHAR".equals(meta.column_types[columnIndex - 1])) {
			return (String) current_chunk[columnIndex - 1].varlen_data[chunk_idx - 1];
		}
		return getObject(columnIndex).toString();
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
		if ("BOOLEAN".equals(meta.column_types[columnIndex - 1])) {
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
		if ("TINYINT".equals(meta.column_types[columnIndex - 1])) {
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
		if ("SMALLINT".equals(meta.column_types[columnIndex - 1])) {
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
		if ("INTEGER".equals(meta.column_types[columnIndex - 1])) {
			return getbuf(columnIndex, 4).getInt();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).intValue();
		}
		return Integer.parseInt(o.toString());
	}

	public long getLong(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return 0;
		}
		if ("BIGINT".equals(meta.column_types[columnIndex - 1])) {
			return getbuf(columnIndex, 8).getLong();
		}
		Object o = getObject(columnIndex);
		if (o instanceof Number) {
			return ((Number) o).longValue();
		}
		return Long.parseLong(o.toString());
	}

	public float getFloat(int columnIndex) throws SQLException {
		if (check_and_null(columnIndex)) {
			return Float.NaN;
		}
		if ("FLOAT".equals(meta.column_types[columnIndex - 1])) {
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
		if ("DOUBLE".equals(meta.column_types[columnIndex - 1])) {
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
		throw new SQLFeatureNotSupportedException();
	}

	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	public Time getTime(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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
		throw new SQLFeatureNotSupportedException();
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
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

	public Blob getBlob(int columnIndex) throws SQLException {
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

	public Blob getBlob(String columnLabel) throws SQLException {
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
		throw new SQLFeatureNotSupportedException();
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
