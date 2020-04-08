package nl.cwi.da.duckdb.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class TestDuckDBJDBC {

	private static void assertTrue(boolean val) throws Exception {
		if (!val) {
			throw new Exception();
		}
	}

	private static void assertFalse(boolean val) throws Exception {
		assertTrue(!val);
	}

	private static void assertEquals(Object a, Object b) throws Exception {
		assertTrue(a.equals(b));
	}
	
	private static void assertEquals(double a, double b, double epsilon) throws Exception {
		assertTrue(Math.abs(a-b) < epsilon);
	}

	private static void fail() throws Exception {
		assertTrue(false);
	}

	static {
		try {
			Class.forName("nl.cwi.da.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void test_connection() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		assertTrue(conn.isValid(0));
		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();
		assertTrue(stmt.isClosed()); // no query yet

		ResultSet rs = stmt.executeQuery("SELECT 42 as a");
		assertFalse(stmt.isClosed());
		assertFalse(rs.isClosed());

		assertTrue(rs.next());
		int res = rs.getInt(1);
		assertEquals(res, 42);
		assertFalse(rs.wasNull());

		res = rs.getInt(1);
		assertEquals(res, 42);
		assertFalse(rs.wasNull());

		res = rs.getInt("a");
		assertEquals(res, 42);
		assertFalse(rs.wasNull());

		try {
			res = rs.getInt(0);
			fail();
		} catch (Exception e) {
		}

		try {
			res = rs.getInt(2);
			fail();
		} catch (Exception e) {
		}

		try {
			res = rs.getInt("b");
			fail();
		} catch (Exception e) {
		}

		assertFalse(rs.next());
		assertFalse(rs.next());

		rs.close();
		rs.close();
		assertTrue(rs.isClosed());

		try {
			res = rs.getInt(1);
			fail();
		} catch (Exception e) {
		}

		stmt.close();
		stmt.close();
		assertTrue(stmt.isClosed());

		conn.close();
		conn.close();
		assertFalse(conn.isValid(0));
		assertTrue(conn.isClosed());

		try {
			stmt = conn.createStatement();
			fail();
		} catch (Exception e) {
		}

	}

	public static void test_result() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs;

		rs = stmt.executeQuery("SELECT CAST(42 AS INTEGER) as a, CAST(4.2 AS DOUBLE) as b");
		ResultSetMetaData meta = rs.getMetaData();
		assertEquals(meta.getColumnCount(), 2);
		assertEquals(meta.getColumnName(1), "a");
		assertEquals(meta.getColumnName(2), "b");
		assertEquals(meta.getColumnTypeName(1), "INTEGER");
		assertEquals(meta.getColumnTypeName(2), "DOUBLE");

		try {
			meta.getColumnName(0);
			fail();
		} catch (Exception e) {
		}

		try {
			meta.getColumnTypeName(0);
			fail();
		} catch (Exception e) {
		}

		try {
			meta.getColumnName(3);
			fail();
		} catch (Exception e) {
		}

		try {
			meta.getColumnTypeName(3);
			fail();
		} catch (Exception e) {
		}

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 42);
		assertEquals(rs.getString(1), "42");
		assertEquals(rs.getDouble(1), 42.0, 0.001);
		assertTrue(rs.getObject(1).equals(new Integer(42)));

		assertEquals(rs.getInt("a"), 42);
		assertEquals(rs.getString("a"), "42");
		assertEquals(rs.getDouble("a"), 42.0, 0.001);
		assertTrue(rs.getObject("a").equals(new Integer(42)));

		assertEquals(rs.getInt(2), 4);
		assertEquals(rs.getString(2), "4.2");
		assertEquals(rs.getDouble(2), 4.2, 0.001);
		assertTrue(rs.getObject(2).equals(new Double(4.2)));

		assertEquals(rs.getInt("b"), 4);
		assertEquals(rs.getString("b"), "4.2");
	assertEquals(rs.getDouble("b"), 4.2, 0.001);
		assertTrue(rs.getObject("b").equals(new Double(4.2)));

		assertFalse(rs.next());

		rs.close();

		stmt.close();
		conn.close();
	}

	public static void test_empty_table() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE a (i iNTEGER)");
		ResultSet rs = stmt.executeQuery("SELECT * FROM a");
		assertFalse(rs.next());

		try {
			rs.getObject(1);
			fail();
		} catch (Exception e) {
		}

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void main(String[] args) throws Exception {
		test_connection();
		test_result();
		test_empty_table();
		System.out.println("OK");
	}
}
