package org.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.duckdb.test.Assertions.assertEquals;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;

public class TestExtensionTypes {

    public static void test_extension_type() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement stmt = connection.createStatement()) {

            DuckDBNative.duckdb_jdbc_create_extension_type((DuckDBConnection) connection);

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT {\"hello\": 'foo', \"world\": 'bar'}::test_type, '\\xAA'::byte_test_type")) {
                rs.next();
                assertEquals(rs.getObject(1), "{'hello': foo, 'world': bar}");
                assertEquals(rs.getObject(2), "\\xAA");
            }
        }
    }

    public static void test_extension_type_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();) {
            DuckDBNative.duckdb_jdbc_create_extension_type((DuckDBConnection) conn);

            stmt.execute("CREATE TABLE test (foo test_type, bar byte_test_type);");
            stmt.execute("INSERT INTO test VALUES ({\"hello\": 'foo', \"world\": 'bar'}, '\\xAA');");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(meta.getColumnCount(), 2);

                assertEquals(meta.getColumnName(1), "foo");
                assertEquals(meta.getColumnTypeName(1), "test_type");
                assertEquals(meta.getColumnType(1), Types.JAVA_OBJECT);
                assertEquals(meta.getColumnClassName(1), "java.lang.String");

                assertEquals(meta.getColumnName(2), "bar");
                assertEquals(meta.getColumnTypeName(2), "byte_test_type");
                assertEquals(meta.getColumnType(2), Types.JAVA_OBJECT);
                assertEquals(meta.getColumnClassName(2), "java.lang.String");
            }
        }
    }
}
