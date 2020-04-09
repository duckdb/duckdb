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
		assertTrue(Math.abs(a - b) < epsilon);
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

	public static void test_broken_next() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE t0(c0 INT8, c1 VARCHAR)");
		stmt.execute(
				"INSERT INTO t0(c1, c0) VALUES (-315929644, 1), (-315929644, -315929644), (-634993846, -1981637379)");
		stmt.execute("INSERT INTO t0(c0, c1) VALUES (-433000283, -433000283)");
		stmt.execute("INSERT INTO t0(c0) VALUES (-995217820)");
		stmt.execute("INSERT INTO t0(c1, c0) VALUES (-315929644, -315929644)");

		ResultSet rs = stmt.executeQuery("SELECT c0 FROM t0");
		while (rs.next()) {
			assertTrue(!rs.getObject(1).equals(null));
		}

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_multiple_connections() throws Exception {
		Connection conn1 = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt1 = conn1.createStatement();
		Connection conn2 = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt2 = conn2.createStatement();
		Statement stmt3 = conn2.createStatement();

		ResultSet rs1 = stmt1.executeQuery("SELECT 42");
		assertTrue(rs1.next());
		assertEquals(42, rs1.getInt(1));
		rs1.close();

		ResultSet rs2 = stmt2.executeQuery("SELECT 43");
		assertTrue(rs2.next());
		assertEquals(43, rs2.getInt(1));

		ResultSet rs3 = stmt3.executeQuery("SELECT 44");
		assertTrue(rs3.next());
		assertEquals(44, rs3.getInt(1));
		rs3.close();

		// creative closing sequence should also work
		stmt2.close();

		rs3 = stmt3.executeQuery("SELECT 44");
		assertTrue(rs3.next());
		assertEquals(44, rs3.getInt(1));

		stmt2.close();
		rs2.close();
		rs3.close();

		System.gc();
		System.gc();

		// stmt1 still works
		rs1 = stmt1.executeQuery("SELECT 42");
		assertTrue(rs1.next());
		assertEquals(42, rs1.getInt(1));
		rs1.close();

		// stmt3 still works
		rs3 = stmt3.executeQuery("SELECT 42");
		assertTrue(rs3.next());
		assertEquals(42, rs3.getInt(1));
		rs3.close();

		conn2.close();

		stmt3.close();

		rs2 = null;
		rs3 = null;
		stmt2 = null;
		stmt3 = null;
		conn2 = null;

		System.gc();
		System.gc();

		// stmt1 still works
		rs1 = stmt1.executeQuery("SELECT 42");
		assertTrue(rs1.next());
		assertEquals(42, rs1.getInt(1));
		rs1.close();
		conn1.close();
		stmt1.close();
	}

	public static void test_big_data() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		int rows = 10000;
		stmt.execute("CREATE TABLE a (i iNTEGER)");
		for (int i = 0; i < rows; i++) {
			stmt.execute("INSERT INTO a VALUES (" + i + ")");
		}

		ResultSet rs = stmt.executeQuery(
				"SELECT CAST(i AS SMALLINT), CAST(i AS INTEGER), CAST(i AS BIGINT), CAST(i AS FLOAT), CAST(i AS DOUBLE), CAST(i as STRING), NULL FROM a");
		int count = 0;
		while (rs.next()) {
			for (int col = 1; col <= 6; col++) {
				assertEquals(rs.getShort(col), (short) count);
				assertFalse(rs.wasNull());
				assertEquals(rs.getInt(col), (int) count);
				assertFalse(rs.wasNull());
				assertEquals(rs.getLong(col), (long) count);
				assertFalse(rs.wasNull());
				assertEquals(rs.getFloat(col), (float) count, 0.001);
				assertFalse(rs.wasNull());
				assertEquals(rs.getDouble(col), (double) count, 0.001);
				assertFalse(rs.wasNull());
				assertEquals(Double.parseDouble(rs.getString(col)), (double) count, 0.001);
				assertFalse(rs.wasNull());
				Object o = rs.getObject(col);
				assertFalse(rs.wasNull());
			}
			short null_short = rs.getShort(7);
			assertTrue(rs.wasNull());
			int null_int = rs.getInt(7);
			assertTrue(rs.wasNull());
			long null_long = rs.getLong(7);
			assertTrue(rs.wasNull());
			float null_float = rs.getFloat(7);
			assertTrue(rs.wasNull());
			double null_double = rs.getDouble(7);
			assertTrue(rs.wasNull());
			String null_string = rs.getString(7);
			assertTrue(rs.wasNull());
			Object null_object = rs.getObject(7);
			assertTrue(rs.wasNull());

			count++;
		}

		assertEquals(rows, count);

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void main(String[] args) throws Exception {
		test_connection();
		test_result();
		test_empty_table();
		test_broken_next();
		test_multiple_connections();
		test_big_data();
		System.out.println("OK");
	}
}
