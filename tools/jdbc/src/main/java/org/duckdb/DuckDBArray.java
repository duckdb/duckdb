package org.duckdb;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'free'");
    }

    @Override
    public Object getArray() throws SQLException {
        Object[] out = new Object[length];
        for (int i=0; i<length; i++) {
            out[i] = vector.getObject(offset + i);
        }
        return out;
    }

    @Override
    public Object getArray(Map<String, Class<?>> arg0) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public Object getArray(long arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public Object getArray(long arg0, int arg1, Map<String, Class<?>> arg2) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getArray'");
    }

    @Override
    public int getBaseType() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBaseType'");
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBaseTypeName'");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> arg0) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(long arg0, int arg1) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }

    @Override
    public ResultSet getResultSet(long arg0, int arg1, Map<String, Class<?>> arg2) throws SQLException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResultSet'");
    }
}
