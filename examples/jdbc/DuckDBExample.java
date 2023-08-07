import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DuckDBExample {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("org.duckdb.DuckDBDriver");

        // this JDBC url creates a temporary in-memory database. If you want to use a
        // persistent DB, append its file name
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE test (a INTEGER, b VARCHAR)");

        PreparedStatement p_stmt = conn.prepareStatement("INSERT INTO test VALUES (?, ?);");

        p_stmt.setInt(1, 42);
        p_stmt.setString(2, "Hello");
        p_stmt.execute();

        p_stmt.setInt(1, 43);
        p_stmt.setString(2, "World");
        p_stmt.execute();

        p_stmt.close();

        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        ResultSetMetaData md = rs.getMetaData();
        int row = 1;
        while (rs.next()) {
            for (int col = 1; col <= md.getColumnCount(); col++) {
                System.out.println(md.getColumnName(col) + "[" + row + "]=" + rs.getString(col) + " (" +
                                   md.getColumnTypeName(col) + ")");
            }
            row++;
        }

        rs.close();
        stmt.close();
        conn.close();
    }
}
