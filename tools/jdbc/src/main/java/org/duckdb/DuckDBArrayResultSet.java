package org.duckdb;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class DuckDBArrayResultSet implements ResultSet {

    private DuckDBVector vector;
    int offset, length;

    int currentValueIndex = -1;
    boolean closed = false;
    boolean wasNull = false;

    public DuckDBArrayResultSet(DuckDBVector vector, int offset, int length) {
        this.vector = vector;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public boolean next() {
        ++currentValueIndex;
        boolean hasNext = currentValueIndex >= 0 && currentValueIndex < length;
        checkBounds();
        return hasNext;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private <T> T getValue(String columnLabel, SqlValueGetter<T> getter) throws SQLException {
        return getValue(findColumn(columnLabel), getter);
    }

    private <T> T getValue(int columnIndex, SqlValueGetter<T> getter) throws SQLException {
        if (columnIndex == 1) {
            throw new IllegalArgumentException(
                "The first element of Array-backed ResultSet can only be retrieved with getInt()");
        }
        if (columnIndex != 2) {
            throw new IllegalArgumentException("Array-backed ResultSet can only have two columns");
        }
        T value = getter.getValue(offset + currentValueIndex);

        wasNull = value == null;
        return value;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getLazyString);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getBoolean);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getByte);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getShort);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (columnIndex == 1) {
            wasNull = false;
            return currentValueIndex + 1;
        }
        return getValue(columnIndex, vector::getInt);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getInt(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getFloat);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getDouble);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValue(columnIndex, vector::getBigDecimal);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getDate);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getTime);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getTimestamp);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getLazyString);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getBoolean);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getByte);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getShort);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        if (columnIndex == 1) {
            return currentValueIndex;
        }
        return getValue(columnIndex, vector::getInt);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getInt(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getFloat);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getDouble);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue(columnLabel, vector::getBigDecimal);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getDate);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getTime);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getTimestamp);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMetaData");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getObject);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getTimestamp);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return Integer.parseInt(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getBigDecimal);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getBigDecimal);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentValueIndex < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentValueIndex >= length;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentValueIndex == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentValueIndex == length - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        currentValueIndex = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        currentValueIndex = length;
    }

    @Override
    public boolean first() throws SQLException {
        if (length > 0) {
            currentValueIndex = 0;
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        if (length > 0) {
            currentValueIndex = length - 1;
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return currentValueIndex + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (row >= 0) {
            currentValueIndex = row - 1;
        } else {
            currentValueIndex = length + row;
        }

        return checkBounds();
    }

    private boolean checkBounds() {
        if (currentValueIndex < -1) {
            currentValueIndex = -1;
            return false;
        }
        if (currentValueIndex > length) {
            currentValueIndex = length;
            return false;
        }
        return true;
    }
    @Override
    public boolean relative(int rows) throws SQLException {
        currentValueIndex += rows;
        return checkBounds();
    }

    @Override
    public boolean previous() throws SQLException {
        --currentValueIndex;
        return checkBounds();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // do nothing
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // do nothing
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("insertRow");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRow");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("deleteRow");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow");
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException("getStatement");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getValue(columnIndex, vector::getArray);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getValue(columnLabel, vector::getObject);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getValue(columnLabel, vector::getArray);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}

/**
 * Extracts a value of requested type given a column index.
 * IntFunction unsuitable because of the checked exception.
 * @param <T> Type of value to extract
 */
interface SqlValueGetter<T> {
    T getValue(int index) throws SQLException;
}
