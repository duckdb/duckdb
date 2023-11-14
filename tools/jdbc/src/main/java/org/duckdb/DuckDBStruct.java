package org.duckdb;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import java.util.HashMap;

public class DuckDBStruct implements Struct {
    private final Object[] attributes;
    private final String[] keys;
    private final DuckDBVector[] values;
    private final int offset;
    private final String typeName;

    DuckDBStruct(String[] keys, DuckDBVector[] values, int offset, String typeName) throws SQLException {
        this.keys = keys;
        this.values = values;
        this.offset = offset;
        this.typeName = typeName;

        attributes = new Object[this.keys.length];
        for (int i = 0; i < this.keys.length; i++) {
            attributes[i] = this.values[i].getObject(this.offset);
        }
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return getAttributes();
    }

    public Map<String, Object> getMap() throws SQLException {
        Object[] values = getAttributes();
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            result.put(keys[i], values[i]);
        }
        return result;
    }

    @Override
    public String toString() {
        Object v = null;
        try {
            v = getMap();
        } catch (SQLException e) {
            v = e;
        }
        return v.toString();
    }
}
