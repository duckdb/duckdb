package org.duckdb;

import java.sql.SQLException;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

final class JdbcUtils {

    @SuppressWarnings("unchecked")
    static <T> T unwrap(Object obj, Class<T> iface) throws SQLException {
        if (!iface.isInstance(obj)) {
            throw new SQLException(obj.getClass().getName() + " not unwrappable from " + iface.getName());
        }
        return (T) obj;
    }

    static byte[] readAllBytes(InputStream x) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] thing = new byte[256];
        while (x.read(thing) != 0) {
            out.write(thing);
        }
        return out.toByteArray();
    }

    private JdbcUtils() {
    }
}
