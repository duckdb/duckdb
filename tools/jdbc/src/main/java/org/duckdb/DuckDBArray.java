package org.duckdb;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.duckdb.DuckDBResultSetMetaData.type_to_int;

public class DuckDBArray implements Array {
    private DuckDBVector vector;
    int offset, length;

    public DuckDBArray(DuckDBVector vector, int offset, int length) {
        this.vector = vector;
        this.length = length;
        this.offset = offset;
    }

    @Override
    public void free() throws SQLException {
        // we don't own the vector, so cannot free it
    }
    @Override
    public Object getArray() throws SQLException {
        Object[] out = new Object[length];
        for (int i = 0; i < length; i++) {
            out[i] = vector.getObject(offset + i);
        }
        return out;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public int getBaseType() throws SQLException {
        return type_to_int(vector.duckdb_type);
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return vector.duckdb_type.name();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }
}
