package org.duckdb.test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.SQLWarning;
import java.util.Properties;
import java.util.TimeZone;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDatabase;
import org.duckdb.DuckDBDriver;
import org.duckdb.DuckDBTimestamp;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBResultSetMetaData;

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
		if (a == null && b == null) {
			return;
		}
		assertTrue(a.equals(b));
	}

	private static void assertNull(Object a) throws Exception {
		assertTrue(a == null);
	}

	private static void assertEquals(double a, double b, double epsilon) throws Exception {
		assertTrue(Math.abs(a - b) < epsilon);
	}

	private static void fail() throws Exception {
		assertTrue(false);
	}

	static {
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void test_connection() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		assertTrue(conn.isValid(0));
		assertFalse(conn.isClosed());

		Statement stmt = conn.createStatement();

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
		} catch (SQLException e) {
		}

		try {
			res = rs.getInt(2);
			fail();
		} catch (SQLException e) {
		}

		try {
			res = rs.getInt("b");
			fail();
		} catch (SQLException e) {
		}

		assertFalse(rs.next());
		assertFalse(rs.next());

		rs.close();
		rs.close();
		assertTrue(rs.isClosed());

		try {
			res = rs.getInt(1);
			fail();
		} catch (SQLException e) {
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
		} catch (SQLException e) {
		}

	}

	public static void test_prepare_exception() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
	
		stmt = conn.createStatement();
		
		try {
			stmt.execute("this is no SQL;");
			fail();
		} catch (SQLException e) {
		}
	}

	public static void test_execute_exception() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
	
		stmt = conn.createStatement();
		stmt.execute("CREATE TABLE t (id INT, b UUID)");
		stmt.execute("INSERT INTO t VALUES (1, uuid())");
		
		try {
			ResultSet rs = stmt.executeQuery("SELECT * FROM t");
			rs.next();
			fail();
		} catch (SQLException e) {
		}
	}

	public static void test_autocommit_off() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		ResultSet rs;

		conn.setAutoCommit(false);
	
		stmt = conn.createStatement();
		stmt.execute("CREATE TABLE t (id INT);");
		conn.commit();

		stmt.execute("INSERT INTO t (id) VALUES (1);");
		stmt.execute("INSERT INTO t (id) VALUES (2);");
		stmt.execute("INSERT INTO t (id) VALUES (3);");
		conn.commit();

		rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
		rs.next();
		assertEquals(rs.getInt(1), 3);
		rs.close();

		stmt.execute("INSERT INTO t (id) VALUES (4);");
		stmt.execute("INSERT INTO t (id) VALUES (5);");
		conn.rollback();

		// After the rollback both inserts must be reverted
		rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
		rs.next();
		assertEquals(rs.getInt(1), 3);
		
		stmt.execute("INSERT INTO t (id) VALUES (6);");
		stmt.execute("INSERT INTO t (id) VALUES (7);");
		
		conn.setAutoCommit(true);

		// Turning auto-commit on triggers a commit
		rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
		rs.next();
		assertEquals(rs.getInt(1), 5);
		
		// This means a rollback must not be possible now
		try {
			conn.rollback();
			fail();
		} catch (SQLException e) {}

		stmt.execute("INSERT INTO t (id) VALUES (8);");
		rs = stmt.executeQuery("SELECT COUNT(*) FROM T");
		rs.next();
		assertEquals(rs.getInt(1), 6);

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_enum() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs;

		// Test 8 bit enum + different access ways
		stmt.execute("CREATE TYPE enum_test AS ENUM ('Enum1', 'enum2', '1üöñ');");
		stmt.execute("CREATE TABLE t (id INT, e1 enum_test);");
		stmt.execute("INSERT INTO t (id, e1) VALUES (1, 'Enum1');");
		stmt.execute("INSERT INTO t (id, e1) VALUES (2, 'enum2');");
		stmt.execute("INSERT INTO t (id, e1) VALUES (3, '1üöñ');");

		PreparedStatement ps = conn.prepareStatement("SELECT e1 FROM t WHERE id = ?");
		ps.setObject(1, 1);
		rs = ps.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1, String.class).equals("Enum1"));
		assertTrue(rs.getString(1).equals("Enum1"));
		assertTrue(rs.getString("e1").equals("Enum1"));
		rs.close();

		ps.setObject(1, 2);
		rs = ps.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1, String.class).equals("enum2"));
		assertTrue(rs.getObject(1).equals("enum2"));
		rs.close();

		ps.setObject(1, 3);
		rs = ps.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1, String.class).equals("1üöñ"));
		assertTrue(rs.getObject(1).equals("1üöñ"));
		assertTrue(rs.getObject("e1").equals("1üöñ"));
		rs.close();

		ps = conn.prepareStatement("SELECT e1 FROM t WHERE e1 = ?");
		ps.setObject(1, "1üöñ");
		rs = ps.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1, String.class).equals("1üöñ"));
		assertTrue(rs.getString(1).equals("1üöñ"));
		assertTrue(rs.getString("e1").equals("1üöñ"));
		rs.close();

		// Test 16 bit enum
		stmt.execute("CREATE TYPE enum_long AS ENUM ('enum0' ,'enum1' ,'enum2' ,'enum3' ,'enum4' ,'enum5' ,'enum6'"
		+ ",'enum7' ,'enum8' ,'enum9' ,'enum10' ,'enum11' ,'enum12' ,'enum13' ,'enum14' ,'enum15' ,'enum16' ,'enum17'"
		+ ",'enum18' ,'enum19' ,'enum20' ,'enum21' ,'enum22' ,'enum23' ,'enum24' ,'enum25' ,'enum26' ,'enum27' ,'enum28'"
		+ ",'enum29' ,'enum30' ,'enum31' ,'enum32' ,'enum33' ,'enum34' ,'enum35' ,'enum36' ,'enum37' ,'enum38' ,'enum39'"
		+ ",'enum40' ,'enum41' ,'enum42' ,'enum43' ,'enum44' ,'enum45' ,'enum46' ,'enum47' ,'enum48' ,'enum49' ,'enum50'"
		+ ",'enum51' ,'enum52' ,'enum53' ,'enum54' ,'enum55' ,'enum56' ,'enum57' ,'enum58' ,'enum59' ,'enum60' ,'enum61'"
		+ ",'enum62' ,'enum63' ,'enum64' ,'enum65' ,'enum66' ,'enum67' ,'enum68' ,'enum69' ,'enum70' ,'enum71' ,'enum72'"
		+ ",'enum73' ,'enum74' ,'enum75' ,'enum76' ,'enum77' ,'enum78' ,'enum79' ,'enum80' ,'enum81' ,'enum82' ,'enum83'"
		+ ",'enum84' ,'enum85' ,'enum86' ,'enum87' ,'enum88' ,'enum89' ,'enum90' ,'enum91' ,'enum92' ,'enum93' ,'enum94'"
		+ ",'enum95' ,'enum96' ,'enum97' ,'enum98' ,'enum99' ,'enum100' ,'enum101' ,'enum102' ,'enum103' ,'enum104' "
		+ ",'enum105' ,'enum106' ,'enum107' ,'enum108' ,'enum109' ,'enum110' ,'enum111' ,'enum112' ,'enum113' ,'enum114'"
		+ ",'enum115' ,'enum116' ,'enum117' ,'enum118' ,'enum119' ,'enum120' ,'enum121' ,'enum122' ,'enum123' ,'enum124'"
		+ ",'enum125' ,'enum126' ,'enum127' ,'enum128' ,'enum129' ,'enum130' ,'enum131' ,'enum132' ,'enum133' ,'enum134'"
		+ ",'enum135' ,'enum136' ,'enum137' ,'enum138' ,'enum139' ,'enum140' ,'enum141' ,'enum142' ,'enum143' ,'enum144'"
		+ ",'enum145' ,'enum146' ,'enum147' ,'enum148' ,'enum149' ,'enum150' ,'enum151' ,'enum152' ,'enum153' ,'enum154'"
		+ ",'enum155' ,'enum156' ,'enum157' ,'enum158' ,'enum159' ,'enum160' ,'enum161' ,'enum162' ,'enum163' ,'enum164'"
		+ ",'enum165' ,'enum166' ,'enum167' ,'enum168' ,'enum169' ,'enum170' ,'enum171' ,'enum172' ,'enum173' ,'enum174'"
		+ ",'enum175' ,'enum176' ,'enum177' ,'enum178' ,'enum179' ,'enum180' ,'enum181' ,'enum182' ,'enum183' ,'enum184'"
		+ ",'enum185' ,'enum186' ,'enum187' ,'enum188' ,'enum189' ,'enum190' ,'enum191' ,'enum192' ,'enum193' ,'enum194'"
		+ ",'enum195' ,'enum196' ,'enum197' ,'enum198' ,'enum199' ,'enum200' ,'enum201' ,'enum202' ,'enum203' ,'enum204'"
		+ ",'enum205' ,'enum206' ,'enum207' ,'enum208' ,'enum209' ,'enum210' ,'enum211' ,'enum212' ,'enum213' ,'enum214'"
		+ ",'enum215' ,'enum216' ,'enum217' ,'enum218' ,'enum219' ,'enum220' ,'enum221' ,'enum222' ,'enum223' ,'enum224'"
		+ ",'enum225' ,'enum226' ,'enum227' ,'enum228' ,'enum229' ,'enum230' ,'enum231' ,'enum232' ,'enum233' ,'enum234'"
		+ ",'enum235' ,'enum236' ,'enum237' ,'enum238' ,'enum239' ,'enum240' ,'enum241' ,'enum242' ,'enum243' ,'enum244'"
		+ ",'enum245' ,'enum246' ,'enum247' ,'enum248' ,'enum249' ,'enum250' ,'enum251' ,'enum252' ,'enum253' ,'enum254'"
		+ ",'enum255' ,'enum256' ,'enum257' ,'enum258' ,'enum259' ,'enum260' ,'enum261' ,'enum262' ,'enum263' ,'enum264'"
		+ ",'enum265' ,'enum266' ,'enum267' ,'enum268' ,'enum269' ,'enum270' ,'enum271' ,'enum272' ,'enum273' ,'enum274'"
		+ ",'enum275' ,'enum276' ,'enum277' ,'enum278' ,'enum279' ,'enum280' ,'enum281' ,'enum282' ,'enum283' ,'enum284'"
		+ ",'enum285' ,'enum286' ,'enum287' ,'enum288' ,'enum289' ,'enum290' ,'enum291' ,'enum292' ,'enum293' ,'enum294'"
		+ ",'enum295' ,'enum296' ,'enum297' ,'enum298' ,'enum299');");

		stmt.execute("CREATE TABLE t2 (id INT, e1 enum_long);");
		stmt.execute("INSERT INTO t2 (id, e1) VALUES (1, 'enum290');");

		ps = conn.prepareStatement("SELECT e1 FROM t2 WHERE id = ?");
		ps.setObject(1, 1);
		rs = ps.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1, String.class).equals("enum290"));
		assertTrue(rs.getString(1).equals("enum290"));
		assertTrue(rs.getString("e1").equals("enum290"));
		rs.close();
		conn.close();
	}

	public static void test_timestamp_tz() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs;

		stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ)");
		stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10+02')");
		stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:10')");

		PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO T (id, t1) VALUES (?, ?)");

		OffsetDateTime odt1 = OffsetDateTime.of(2020, 10, 7, 13, 15, 7, 12345, ZoneOffset.ofHours(7));
		OffsetDateTime odt1Rounded = OffsetDateTime.of(2020, 10, 7, 13, 15, 7, 12000, ZoneOffset.ofHours(7));
		OffsetDateTime odt2 = OffsetDateTime.of(1878, 10, 2, 1, 15, 7, 12345, ZoneOffset.ofHours(-5));
		OffsetDateTime odt2Rounded = OffsetDateTime.of(1878, 10, 2, 1, 15, 7, 13000, ZoneOffset.ofHours(-5));
		OffsetDateTime odt3 = OffsetDateTime.of(2022, 1, 1, 12, 11, 10, 0, ZoneOffset.ofHours(2));
		OffsetDateTime odt4 = OffsetDateTime.of(2022, 1, 1, 12, 11, 10, 0, ZoneOffset.ofHours(0));
		OffsetDateTime odt5 = OffsetDateTime.of(1900, 11, 27, 23, 59, 59, 0, ZoneOffset.ofHours(1));

		ps.setObject(1, 3);
		ps.setObject(2, odt1);
		ps.execute();
		ps.setObject(1, 4);
		ps.setObject(2, odt5, Types.TIMESTAMP_WITH_TIMEZONE);
		ps.execute();
		ps.setObject(1, 5);
		ps.setObject(2, odt2);
		ps.execute();

		rs = stmt.executeQuery("SELECT * FROM t ORDER BY id");
		ResultSetMetaData meta = rs.getMetaData();
		rs.next();
		assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt3));
		rs.next();
		assertEquals(rs.getObject(2, OffsetDateTime.class), odt4);
		rs.next();
		assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt1Rounded));
		rs.next();
		assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt5));
		rs.next();
		assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt2Rounded));
		assertTrue(((OffsetDateTime)rs.getObject(2)).isEqual(odt2Rounded));

		// Metadata tests
		assertEquals(Types.TIME_WITH_TIMEZONE, ((DuckDBResultSetMetaData)meta).type_to_int(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
		assertTrue(OffsetDateTime.class.toString().equals(meta.getColumnClassName(2)));

		rs.close();
		stmt.close();
		conn.close();
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
		} catch (ArrayIndexOutOfBoundsException e) {
		}

		try {
			meta.getColumnTypeName(0);
			fail();
		} catch (ArrayIndexOutOfBoundsException e) {
		}

		try {
			meta.getColumnName(3);
			fail();
		} catch (SQLException e) {
		}

		try {
			meta.getColumnTypeName(3);
			fail();
		} catch (SQLException e) {
		}

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 42);
		assertEquals(rs.getString(1), "42");
		assertEquals(rs.getDouble(1), 42.0, 0.001);
		assertTrue(rs.getObject(1).equals(42));

		assertEquals(rs.getInt("a"), 42);
		assertEquals(rs.getString("a"), "42");
		assertEquals(rs.getDouble("a"), 42.0, 0.001);
		assertTrue(rs.getObject("a").equals(42));

		assertEquals(rs.getInt(2), 4);
		assertEquals(rs.getString(2), "4.2");
		assertEquals(rs.getDouble(2), 4.2, 0.001);
		assertTrue(rs.getObject(2).equals(4.2));

		assertEquals(rs.getInt("b"), 4);
		assertEquals(rs.getString("b"), "4.2");
		assertEquals(rs.getDouble("b"), 4.2, 0.001);
		assertTrue(rs.getObject("b").equals(4.2));

		assertFalse(rs.next());

		rs.close();

		stmt.close();
		// test duplication
		Connection conn2 = ((DuckDBConnection) conn).duplicate();
		ResultSet rs_conn2 = conn2.createStatement().executeQuery("SELECT 42");
		rs_conn2.next();
		assertEquals(42, rs_conn2.getInt(1));
		rs_conn2.close();
		conn.close();
		conn2.close();
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
		} catch (ArrayIndexOutOfBoundsException e) {
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

	public static void test_duckdb_timestamp() throws Exception {

		duckdb_timestamp_test();

		// Store default time zone
		TimeZone defaultTZ = TimeZone.getDefault();

		// Test with different time zones
		TimeZone.setDefault(TimeZone.getTimeZone("America/Lima"));
		duckdb_timestamp_test();

		// Test with different time zones
		TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
		duckdb_timestamp_test();

		// Restore default time zone
		TimeZone.setDefault(defaultTZ);
	}

	public static void duckdb_timestamp_test() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE a (ts TIMESTAMP)");

		// Generat tests without database
		Timestamp ts0 = Timestamp.valueOf("1970-01-01 00:00:00");
		Timestamp ts1 = Timestamp.valueOf("2021-07-29 21:13:11");
		Timestamp ts2 = Timestamp.valueOf("2021-07-29 21:13:11.123456");
		Timestamp ts3 = Timestamp.valueOf("1921-07-29 21:13:11");
		Timestamp ts4 = Timestamp.valueOf("1921-07-29 21:13:11.123456");

		Timestamp cts0 = new DuckDBTimestamp(ts0).toSqlTimestamp();
		Timestamp cts1 = new DuckDBTimestamp(ts1).toSqlTimestamp();
		Timestamp cts2 = new DuckDBTimestamp(ts2).toSqlTimestamp();
		Timestamp cts3 = new DuckDBTimestamp(ts3).toSqlTimestamp();
		Timestamp cts4 = new DuckDBTimestamp(ts4).toSqlTimestamp();

		assertTrue(ts0.getTime() == cts0.getTime());
		assertTrue(ts0.compareTo(cts0) == 0);
		assertTrue(ts1.getTime() == cts1.getTime());
		assertTrue(ts1.compareTo(cts1) == 0);
		assertTrue(ts2.getTime() == cts2.getTime());
		assertTrue(ts2.compareTo(cts2) == 0);
		assertTrue(ts3.getTime() == cts3.getTime());
		assertTrue(ts3.compareTo(cts3) == 0);
		assertTrue(ts4.getTime() == cts4.getTime());
		assertTrue(ts4.compareTo(cts4) == 0);

		assertTrue(DuckDBTimestamp.getMicroseconds(DuckDBTimestamp.toSqlTimestamp(5678912345L)) == 5678912345L);

		DuckDBTimestamp dts4 = new DuckDBTimestamp(ts1);
		assertTrue(dts4.toSqlTimestamp().compareTo(ts1) == 0);
		DuckDBTimestamp dts5 = new DuckDBTimestamp(ts2);
		assertTrue(dts5.toSqlTimestamp().compareTo(ts2) == 0);

		// Insert and read a timestamp
		stmt.execute("INSERT INTO a (ts) VALUES ('2005-11-02 07:59:58')");
		ResultSet rs = stmt.executeQuery(
				"SELECT * FROM a");
		assertTrue(rs.next());
		assertEquals(rs.getObject("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));
		assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));

		rs.close();
		stmt.close();

		PreparedStatement ps = conn.prepareStatement(
				"SELECT COUNT(ts) FROM a WHERE ts = ?");
		ps.setTimestamp(1, Timestamp.valueOf("2005-11-02 07:59:58"));
		ResultSet rs2 = ps.executeQuery();
		assertTrue(rs2.next());
		assertEquals(rs2.getInt(1), 1);
		rs2.close();
		ps.close();

		ps = conn.prepareStatement(
				"SELECT COUNT(ts) FROM a WHERE ts = ?");
		ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"));
		ResultSet rs3 = ps.executeQuery();
		assertTrue(rs3.next());
		assertEquals(rs3.getInt(1), 1);
		rs3.close();
		ps.close();

		ps = conn.prepareStatement(
				"SELECT COUNT(ts) FROM a WHERE ts = ?");
		ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"), Types.TIMESTAMP);
		ResultSet rs4 = ps.executeQuery();
		assertTrue(rs4.next());
		assertEquals(rs4.getInt(1), 1);
		rs4.close();
		ps.close();

		Statement stmt2 = conn.createStatement();
		stmt2.execute("INSERT INTO a (ts) VALUES ('1905-11-02 07:59:58.12345')");
		ps = conn.prepareStatement(
				"SELECT COUNT(ts) FROM a WHERE ts = ?");
		ps.setTimestamp(1, Timestamp.valueOf("1905-11-02 07:59:58.12345"));
		ResultSet rs5 = ps.executeQuery();
		assertTrue(rs5.next());
		assertEquals(rs5.getInt(1), 1);
		rs5.close();
		ps.close();

		ps = conn.prepareStatement(
				"SELECT ts FROM a WHERE ts = ?");
		ps.setTimestamp(1, Timestamp.valueOf("1905-11-02 07:59:58.12345"));
		ResultSet rs6 = ps.executeQuery();
		assertTrue(rs6.next());
		assertEquals(rs6.getTimestamp(1), Timestamp.valueOf("1905-11-02 07:59:58.12345"));
		rs6.close();
		ps.close();

		conn.close();
	}

	public static void test_duckdb_localdatetime() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE x (ts TIMESTAMP)");

		LocalDateTime ldt = LocalDateTime.of(2021,1,18,21,20,7);

		PreparedStatement ps1 = conn.prepareStatement("INSERT INTO x VALUES (?)");
		ps1.setObject(1, ldt);
		ps1.execute();
		ps1.close();

		PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM x");
		ResultSet rs2 = ps2.executeQuery();

		rs2.next();
		assertEquals(rs2.getTimestamp(1), rs2.getObject(1, Timestamp.class));
		assertEquals(rs2.getObject(1, LocalDateTime.class), ldt);

		rs2.close();
		ps2.close();
		stmt.close();
		conn.close();
	}

	public static void test_duckdb_getObject_with_class() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE b (vchar VARCHAR, bo BOOLEAN, sint SMALLINT, nint INTEGER, bigi BIGINT,"
			+ " flt FLOAT, dbl DOUBLE, dte DATE, tme TIME, ts TIMESTAMP, dec16 DECIMAL(3,1),"
			+ " dec32 DECIMAL(9,8), dec64 DECIMAL(16,1), dec128 DECIMAL(30,10), tint TINYINT, utint UTINYINT,"
			+ " usint USMALLINT, uint UINTEGER, ubig UBIGINT, hin HUGEINT, blo BLOB)");
		stmt.execute("INSERT INTO b VALUES ('varchary', true, 6, 42, 666, 42.666, 666.42,"
			+ " '1970-01-02', '01:00:34', '1970-01-03 03:42:23', 42.2, 1.23456789, 987654321012345.6, 111112222233333.44444, "
			+ " -4, 200, 50001, 4000111222, 18446744073709551615, 18446744073709551616, 'yeah'::BLOB)");

		PreparedStatement ps = conn.prepareStatement("SELECT * FROM b");
		ResultSet rs = ps.executeQuery();

		rs.next();
		assertEquals(rs.getString(1), rs.getObject(1, String.class));
		assertEquals(rs.getBoolean(2), rs.getObject(2, Boolean.class));
		assertEquals(rs.getShort(3), rs.getObject(3, Short.class));
		assertEquals(rs.getInt(4), rs.getObject(4, Integer.class));
		assertEquals(rs.getLong(5), rs.getObject(5, Long.class));
		assertEquals(rs.getFloat(6), rs.getObject(6, Float.class));
		assertEquals(rs.getDouble(7), rs.getObject(7, Double.class));
		assertEquals(rs.getDate(8), rs.getObject(8, Date.class));
		assertEquals(rs.getTime(9), rs.getObject(9, Time.class));
		assertEquals(rs.getTimestamp(10), rs.getObject(10, Timestamp.class));
		assertEquals(rs.getObject(10, LocalDateTime.class), LocalDateTime.parse("1970-01-03T03:42:23"));
		assertEquals(rs.getObject(10, LocalDateTime.class), LocalDateTime.of(1970,1,3,3,42,23));
		assertEquals(rs.getBigDecimal(11), rs.getObject(11, BigDecimal.class));
		assertEquals(rs.getBigDecimal(12), rs.getObject(12, BigDecimal.class));
		assertEquals(rs.getBigDecimal(13), rs.getObject(13, BigDecimal.class));
		assertEquals(rs.getBigDecimal(14), rs.getObject(14, BigDecimal.class));

		// Missing implementations, should never reach assertTrue(false)
		try {
			rs.getObject(11, Integer.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(12, Integer.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(13, Integer.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(14, Long.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(15, BigInteger.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(16, BigInteger.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		try {
			rs.getObject(16, Blob.class);
			assertTrue(false);
		}
		catch (SQLException e) {}

		rs.close();
		ps.close();
		stmt.close();
		conn.close();
	}

	public static void test_multiple_statements_execution() throws Exception {
		Connection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery(
						"CREATE TABLE integers(i integer);\n" +
								"insert into integers select * from range(10);" +
								"select * from integers;");
		int i = 0;
		while (rs.next()) {
			assertEquals(rs.getInt("i"), i);
			i++;
		}
		assertEquals(i, 10);
	}

	public static void test_multiple_statements_exception() throws Exception {
		Connection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		boolean succ = false;
		try {
			stmt.executeQuery(
					"CREATE TABLE integers(i integer, i boolean);\n" +
							"CREATE TABLE integers2(i integer);\n" +
							"insert into integers2 select * from range(10);\n" +
							"select * from integers2;");
			succ = true;
		} catch (Exception ex) {
			assertFalse(succ);
		}
	}

	public static void test_bigdecimal() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE q (id DECIMAL(3,0), dec16 DECIMAL(4,1), dec32 DECIMAL(9,4), dec64 DECIMAL(18,7), dec128 DECIMAL(38,10))");

		PreparedStatement ps1 = conn.prepareStatement("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (?, ?, ?, ?, ?)");
		ps1.setObject(1, new BigDecimal("1"));
		ps1.setObject(2, new BigDecimal("999.9"));
		ps1.setObject(3, new BigDecimal("99999.9999"));
		ps1.setObject(4, new BigDecimal("99999999999.9999999"));
		ps1.setObject(5, new BigDecimal("9999999999999999999999999999.9999999999"));
		ps1.execute();

		ps1.clearParameters();
		ps1.setBigDecimal(1, new BigDecimal("2"));
		ps1.setBigDecimal(2, new BigDecimal("-999.9"));
		ps1.setBigDecimal(3, new BigDecimal("-99999.9999"));
		ps1.setBigDecimal(4, new BigDecimal("-99999999999.9999999"));
		ps1.setBigDecimal(5, new BigDecimal("-9999999999999999999999999999.9999999999"));
		ps1.execute();

		ps1.clearParameters();
		ps1.setObject(1, new BigDecimal("3"), Types.DECIMAL);
		ps1.setObject(2, new BigDecimal("-5"), Types.DECIMAL);
		ps1.setObject(3, new BigDecimal("-999"), Types.DECIMAL);
		ps1.setObject(4, new BigDecimal("-88888888"), Types.DECIMAL);
		ps1.setObject(5, new BigDecimal("-123456789654321"), Types.DECIMAL);
		ps1.execute();
		ps1.close();



		stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (4, -0, -0, -0, -0)");
		stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (5, 0, 0, 0, 18446744073709551615)");
		stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (6, 0, 0, 0, 18446744073709551616)");
		stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (7, 0, 0, 0, -18446744073709551615)");
		stmt.execute("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (8, 0, 0, 0, -18446744073709551616)");
		stmt.close();

		PreparedStatement ps = conn.prepareStatement("SELECT * FROM q ORDER BY id");
		ResultSet rs = ps.executeQuery();
		while (rs.next())
		{
			assertEquals(rs.getBigDecimal(1), rs.getObject(1, BigDecimal.class));
			assertEquals(rs.getBigDecimal(2), rs.getObject(2, BigDecimal.class));
			assertEquals(rs.getBigDecimal(3), rs.getObject(3, BigDecimal.class));
			assertEquals(rs.getBigDecimal(4), rs.getObject(4, BigDecimal.class));
			assertEquals(rs.getBigDecimal(5), rs.getObject(5, BigDecimal.class));
		}

		rs.close();

		ResultSet rs2 = ps.executeQuery();
		DuckDBResultSetMetaData meta = (DuckDBResultSetMetaData)rs2.getMetaData();
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("1"));
		assertEquals(rs2.getBigDecimal(2), new BigDecimal("999.9"));
		assertEquals(rs2.getBigDecimal(3), new BigDecimal("99999.9999"));
		assertEquals(rs2.getBigDecimal(4), new BigDecimal("99999999999.9999999"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("9999999999999999999999999999.9999999999"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("2"));
		assertEquals(rs2.getBigDecimal(2), new BigDecimal("-999.9"));
		assertEquals(rs2.getBigDecimal(3), new BigDecimal("-99999.9999"));
		assertEquals(rs2.getBigDecimal(4), new BigDecimal("-99999999999.9999999"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("-9999999999999999999999999999.9999999999"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("3"));
		assertEquals(rs2.getBigDecimal(2), new BigDecimal("-5.0"));
		assertEquals(rs2.getBigDecimal(3), new BigDecimal("-999.0000"));
		assertEquals(rs2.getBigDecimal(4), new BigDecimal("-88888888.0000000"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("-123456789654321.0000000000"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("4"));
		assertEquals(rs2.getBigDecimal(2), new BigDecimal("-0.0"));
		assertEquals(rs2.getBigDecimal(3), new BigDecimal("-0.0000"));
		assertEquals(rs2.getBigDecimal(4), new BigDecimal("-0.0000000"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("-0.0000000000"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("5"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("18446744073709551615.0000000000"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("6"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("18446744073709551616.0000000000"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("7"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("-18446744073709551615.0000000000"));
		rs2.next();
		assertEquals(rs2.getBigDecimal(1), new BigDecimal("8"));
		assertEquals(rs2.getBigDecimal(5), new BigDecimal("-18446744073709551616.0000000000"));
		rs2.close();

		// Metadata tests
		assertEquals(Types.DECIMAL, meta.type_to_int(DuckDBColumnType.DECIMAL));
		assertTrue(BigDecimal.class.toString().equals(meta.getColumnClassName(1)));
		assertTrue(BigDecimal.class.toString().equals(meta.getColumnClassName(2)));
		assertTrue(BigDecimal.class.toString().equals(meta.getColumnClassName(3)));
		assertTrue(BigDecimal.class.toString().equals(meta.getColumnClassName(4)));

		assertEquals(3, meta.getPrecision(1));
		assertEquals(0, meta.getScale(1));
		assertEquals(4, meta.getPrecision(2));
		assertEquals(1, meta.getScale(2));
		assertEquals(9, meta.getPrecision(3));
		assertEquals(4, meta.getScale(3));
		assertEquals(18, meta.getPrecision(4));
		assertEquals(7, meta.getScale(4));
		assertEquals(38, meta.getPrecision(5));
		assertEquals(10, meta.getScale(5));

		conn.close();
	}

	// Longer, resource intensive test - might be commented out for a quick test run
	public static void test_lots_of_timestamps() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE a (ts TIMESTAMP)");

		Timestamp ts = Timestamp.valueOf("1970-01-01 01:01:01");

		for (long i = 134234533L; i < 13423453300L; i=i+73512) {
			ts.setTime(i);
			stmt.execute("INSERT INTO a (ts) VALUES ('" + ts +"')");
		}

		stmt.close();

		for (long i = 134234533L; i < 13423453300L; i=i+73512) {
			PreparedStatement ps = conn.prepareStatement(
					"SELECT COUNT(ts) FROM a WHERE ts = ?");
			ps.setTimestamp(1, ts);
			ResultSet rs = ps.executeQuery();
			assertTrue(rs.next());
			assertEquals(rs.getInt(1), 1);
			rs.close();
			ps.close();
		}

		conn.close();
	}

	public static void test_lots_of_decimals() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE q (id DECIMAL(4,0), dec32 DECIMAL(9,4), dec64 DECIMAL(18,7), dec128 DECIMAL(38,10))");
		stmt.close();

		PreparedStatement ps1 = conn.prepareStatement("INSERT INTO q (id, dec32, dec64, dec128) VALUES (?, ?, ?, ?)");
		ps1.setObject(1, new BigDecimal("1"));

		BigDecimal dec32_org = new BigDecimal("99999.9999");
		BigDecimal dec64_org = new BigDecimal("99999999999.9999999");
		BigDecimal dec128_org = new BigDecimal("9999999999999999999999999999.9999999999");

		ps1.setObject(2, dec32_org);
		ps1.setObject(3, dec64_org);
		ps1.setObject(4, dec128_org);
		ps1.execute();

		PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM q WHERE id = ?");
		BigDecimal multiplicant = new BigDecimal("0.987");

		for (int i = 2; i < 10000; i++) {
			ps2.setObject(1, new BigDecimal(i-1));
			ResultSet rs = ps2.executeQuery();
			assertTrue(rs.next());

			BigDecimal dec32 = rs.getObject(2, BigDecimal.class);
			BigDecimal dec64 = rs.getObject(3, BigDecimal.class);
			BigDecimal dec128 = rs.getObject(4, BigDecimal.class);
			assertEquals(dec32_org, dec32);
			assertEquals(dec64_org, dec64);
			assertEquals(dec128_org, dec128);

			dec32 = rs.getBigDecimal(2);
			dec64 = rs.getBigDecimal(3);
			dec128 = rs.getBigDecimal(4);
			assertEquals(dec32_org, dec32);
			assertEquals(dec64_org, dec64);
			assertEquals(dec128_org, dec128);
			rs.close();

			dec32_org = dec32.multiply(multiplicant).setScale(4, java.math.RoundingMode.HALF_EVEN);
			dec64_org = dec64.multiply(multiplicant).setScale(7, java.math.RoundingMode.HALF_EVEN);
			dec128_org = dec128.multiply(multiplicant).setScale(10, java.math.RoundingMode.HALF_EVEN);

			ps1.clearParameters();
			ps1.setObject(1, new BigDecimal(i));
			ps1.setObject(2, dec32_org);
			ps1.setObject(3, dec64_org);
			ps1.setObject(4, dec128_org);
			ps1.execute();
		}

		ps1.close();
		ps2.close();
		conn.close();
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

	public static void test_crash_bug496() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE t0(c0 BOOLEAN, c1 INT)");
		stmt.execute("CREATE INDEX i0 ON t0(c1, c0)");
		stmt.execute("INSERT INTO t0(c1) VALUES (0)");
		stmt.close();
		conn.close();
	}

	public static void test_tablepragma_bug491() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE t0(c0 INT)");

		ResultSet rs = stmt.executeQuery("PRAGMA table_info('t0')");
		assertTrue(rs.next());

		assertEquals(rs.getInt("cid"), 0);
		assertEquals(rs.getString("name"), "c0");
		assertEquals(rs.getString("type"), "INTEGER");
		assertEquals(rs.getBoolean("notnull"), false);
		rs.getString("dflt_value");
		// assertTrue(rs.wasNull());
		assertEquals(rs.getBoolean("pk"), false);

		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_nulltruth_bug489() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE t0(c0 INT)");
		stmt.execute("INSERT INTO t0(c0) VALUES (0)");

		ResultSet rs = stmt.executeQuery("SELECT * FROM t0 WHERE NOT(NULL OR TRUE)");
		assertFalse(rs.next());

		rs = stmt.executeQuery("SELECT NOT(NULL OR TRUE)");
		assertTrue(rs.next());
		boolean res = rs.getBoolean(1);
		assertEquals(res, false);
		assertFalse(rs.wasNull());

		rs.close();
		stmt.close();
		conn.close();

	}

	public static void test_empty_prepare_bug500() throws Exception {
		String fileContent = "CREATE TABLE t0(c0 VARCHAR, c1 DOUBLE);\n"
				+ "CREATE TABLE t1(c0 DOUBLE, PRIMARY KEY(c0));\n" + "INSERT INTO t0(c0) VALUES (0), (0), (0), (0);\n"
				+ "INSERT INTO t0(c0) VALUES (NULL), (NULL);\n" + "INSERT INTO t1(c0) VALUES (0), (1);\n" + "\n"
				+ "SELECT t0.c0 FROM t0, t1;";
		Connection con = DriverManager.getConnection("jdbc:duckdb:");
		for (String s : fileContent.split("\n")) {
			Statement st = con.createStatement();
			try {
				st.execute(s);
			} catch (SQLException e) {
				// e.printStackTrace();
			}
		}
		con.close();

	}

	public static void test_borked_string_bug539() throws Exception {
		Connection con = DriverManager.getConnection("jdbc:duckdb:");
		Statement s = con.createStatement();
		s.executeUpdate("CREATE TABLE t0 (c0 VARCHAR)");
		String q = String.format("INSERT INTO t0 VALUES('%c')", 55995);
		s.executeUpdate(q);
		s.close();
		con.close();
	}

	public static void test_prepare_types() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");

		PreparedStatement ps = conn.prepareStatement(
				"SELECT CAST(? AS BOOLEAN) c1, CAST(? AS TINYINT) c2, CAST(? AS SMALLINT) c3, CAST(? AS INTEGER) c4, CAST(? AS BIGINT) c5, CAST(? AS FLOAT) c6, CAST(? AS DOUBLE) c7, CAST(? AS STRING) c8");
		ps.setBoolean(1, true);
		ps.setByte(2, (byte) 42);
		ps.setShort(3, (short) 43);
		ps.setInt(4, 44);
		ps.setLong(5, (long) 45);
		ps.setFloat(6, (float) 4.6);
		ps.setDouble(7, (double) 4.7);
		ps.setString(8, "four eight");

		ResultSet rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(rs.getBoolean(1), true);
		assertEquals(rs.getByte(2), (byte) 42);
		assertEquals(rs.getShort(3), (short) 43);
		assertEquals(rs.getInt(4), 44);
		assertEquals(rs.getLong(5), (long) 45);
		assertEquals(rs.getFloat(6), 4.6, 0.001);
		assertEquals(rs.getDouble(7), 4.7, 0.001);
		assertEquals(rs.getString(8), "four eight");
		rs.close();

		ps.setBoolean(1, false);
		ps.setByte(2, (byte) 82);
		ps.setShort(3, (short) 83);
		ps.setInt(4, 84);
		ps.setLong(5, (long) 85);
		ps.setFloat(6, (float) 8.6);
		ps.setDouble(7, (double) 8.7);
		ps.setString(8, "eight eight");

		rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(rs.getBoolean(1), false);
		assertEquals(rs.getByte(2), (byte) 82);
		assertEquals(rs.getShort(3), (short) 83);
		assertEquals(rs.getInt(4), 84);
		assertEquals(rs.getLong(5), (long) 85);
		assertEquals(rs.getFloat(6), 8.6, 0.001);
		assertEquals(rs.getDouble(7), 8.7, 0.001);
		assertEquals(rs.getString(8), "eight eight");
		rs.close();

		ps.setObject(1, false);
		ps.setObject(2, (byte) 82);
		ps.setObject(3, (short) 83);
		ps.setObject(4, 84);
		ps.setObject(5, (long) 85);
		ps.setObject(6, (float) 8.6);
		ps.setObject(7, (double) 8.7);
		ps.setObject(8, "eight eight");

		rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(rs.getBoolean(1), false);
		assertEquals(rs.getByte(2), (byte) 82);
		assertEquals(rs.getShort(3), (short) 83);
		assertEquals(rs.getInt(4), 84);
		assertEquals(rs.getLong(5), (long) 85);
		assertEquals(rs.getFloat(6), 8.6, 0.001);
		assertEquals(rs.getDouble(7), 8.7, 0.001);
		assertEquals(rs.getString(8), "eight eight");

		ps.setNull(1, 0);
		ps.setNull(2, 0);
		ps.setNull(3, 0);
		ps.setNull(4, 0);
		ps.setNull(5, 0);
		ps.setNull(6, 0);
		ps.setNull(7, 0);
		ps.setNull(8, 0);

		rs = ps.executeQuery();
		assertTrue(rs.next());
		assertEquals(8, rs.getMetaData().getColumnCount());
		for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
			assertNull(rs.getObject(c));
			assertTrue(rs.wasNull());
			assertNull(rs.getString(c));
			assertTrue(rs.wasNull());
		}

		rs.close();
		ps.close();
		conn.close();
	}

	public static void test_prepare_insert() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");

		conn.createStatement()
				.executeUpdate("create table ctstable1 (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID))");
		PreparedStatement pStmt1 = conn.prepareStatement("insert into ctstable1 values(?, ?)");
		for (int j = 1; j <= 10; j++) {
			String sTypeDesc = "Type-" + j;
			int newType = j;
			pStmt1.setInt(1, newType);
			pStmt1.setString(2, sTypeDesc);
			int count = pStmt1.executeUpdate();
			assertEquals(count, 1);
		}
		pStmt1.close();

		conn.createStatement().executeUpdate(
				"create table ctstable2 (KEY_ID int, COF_NAME varchar(32), PRICE float, TYPE_ID int, primary key(KEY_ID) )");

		PreparedStatement pStmt = conn.prepareStatement("insert into ctstable2 values(?, ?, ?, ?)");
		for (int i = 1; i <= 10; i++) {
			// Perform the insert(s)
			int newKey = i;
			String newName = "xx" + "-" + i;
			float newPrice = i + (float) .00;
			int newType = i % 5;
			if (newType == 0)
				newType = 5;
			pStmt.setInt(1, newKey);
			pStmt.setString(2, newName);
			pStmt.setFloat(3, newPrice);
			pStmt.setInt(4, newType);
			pStmt.executeUpdate();
		}

		pStmt.close();

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ctstable1");
		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 10);
		rs.close();

		stmt.executeUpdate("DELETE FROM ctstable1");

		rs = stmt.executeQuery("SELECT COUNT(*) FROM ctstable1");
		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 0);
		rs.close();

		stmt.close();

		conn.close();

	}

	public static void test_read_only() throws Exception {
		Path database_file = Files.createTempFile("duckdb-jdbc-test-", ".duckdb");
		database_file.toFile().delete();

		String jdbc_url = "jdbc:duckdb:" + database_file.toString();
		Properties ro_prop = new Properties();
		ro_prop.setProperty("duckdb.read_only", "true");

		Connection conn_rw = DriverManager.getConnection(jdbc_url);
		assertFalse(conn_rw.isReadOnly());
		Statement stmt = conn_rw.createStatement();
		stmt.execute("CREATE TABLE test (i INTEGER)");
		stmt.execute("INSERT INTO test VALUES (42)");
		stmt.close();
		// we cannot create other connections, be it read-write or read-only right now
		try {
			Connection conn2 = DriverManager.getConnection(jdbc_url);
			fail();
		} catch (Exception e) {
		}

		try {
			Connection conn2 = DriverManager.getConnection(jdbc_url, ro_prop);
			fail();
		} catch (Exception e) {
		}

		// hard shutdown to not have to wait on gc
		DuckDBDatabase db = ((DuckDBConnection) conn_rw).getDatabase();
		conn_rw.close();
		db.shutdown();

		try {
			Statement stmt2 = conn_rw.createStatement();
			stmt2.executeQuery("SELECT 42");
			stmt2.close();
			fail();
		} catch (SQLException e) {
		}

		try {
			Connection conn_dup = ((DuckDBConnection) conn_rw).duplicate();
			conn_dup.close();
			fail();
		} catch (SQLException e) {
		}

		// FIXME: requires explicit database shutdown
		// // we can create two parallel read only connections and query them, too
		// Connection conn_ro1 = DriverManager.getConnection(jdbc_url, ro_prop);
		// Connection conn_ro2 = DriverManager.getConnection(jdbc_url, ro_prop);

		// assertTrue(conn_ro1.isReadOnly());
		// assertTrue(conn_ro2.isReadOnly());

		// Statement stmt1 = conn_ro1.createStatement();
		// ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test");
		// rs1.next();
		// assertEquals(rs1.getInt(1), 42);
		// rs1.close();
		// stmt1.close();

		// Statement stmt2 = conn_ro2.createStatement();
		// ResultSet rs2 = stmt2.executeQuery("SELECT * FROM test");
		// rs2.next();
		// assertEquals(rs2.getInt(1), 42);
		// rs2.close();
		// stmt2.close();

		// conn_ro1.close();
		// conn_ro2.close();

	}

	public static void test_hugeint() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery(
				"SELECT 42::hugeint hi1, -42::hugeint hi2, 454564646545646546545646545::hugeint hi3, -454564646545646546545646545::hugeint hi4");
		assertTrue(rs.next());
		assertEquals(rs.getObject("hi1"), new BigInteger("42"));
		assertEquals(rs.getObject("hi2"), new BigInteger("-42"));
		assertEquals(rs.getLong("hi1"), 42L);
		assertEquals(rs.getLong("hi2"), -42L);
		assertEquals(rs.getObject("hi3"), new BigInteger("454564646545646546545646545"));
		assertEquals(rs.getObject("hi4"), new BigInteger("-454564646545646546545646545"));
		assertTrue(rs.getBigDecimal("hi1").compareTo(new BigDecimal("42")) == 0);
		assertTrue(rs.getBigDecimal("hi2").compareTo(new BigDecimal("-42")) == 0);
		assertTrue(rs.getBigDecimal("hi3").compareTo(new BigDecimal("454564646545646546545646545")) == 0);
		assertTrue(rs.getBigDecimal("hi4").compareTo(new BigDecimal("-454564646545646546545646545")) == 0);
		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_exotic_types() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery(
				"SELECT '2019-11-26 21:11:00'::timestamp ts, '2019-11-26'::date dt, interval '5 days' iv, '21:11:00'::time te");
		assertTrue(rs.next());
		assertEquals(rs.getObject("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));
		assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));

		assertEquals(rs.getObject("dt"), Date.valueOf("2019-11-26"));
		assertEquals(rs.getDate("dt"), Date.valueOf("2019-11-26"));

		assertEquals(rs.getObject("iv"), "5 days");

		assertEquals(rs.getObject("te"), Time.valueOf("21:11:00"));
		assertEquals(rs.getTime("te"), Time.valueOf("21:11:00"));

		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_exotic_nulls() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT NULL::timestamp ts, NULL::date dt, NULL::time te");
		assertTrue(rs.next());
		assertNull(rs.getObject("ts"));
		assertNull(rs.getTimestamp("ts"));

		assertNull(rs.getObject("dt"));
		assertNull(rs.getDate("dt"));

		assertNull(rs.getObject("te"));
		assertNull(rs.getTime("te"));

		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_evil_date() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT '5131-08-05 (BC)'::date d");

		assertTrue(rs.next());
		assertNull(rs.getDate("d"));

		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_decimal() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT '1.23'::decimal(3,2) d");

		assertTrue(rs.next());
		assertEquals(rs.getDouble("d"), 1.23);

		assertFalse(rs.next());
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_schema_reflection() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE a (i INTEGER)");
		stmt.execute("CREATE VIEW b AS SELECT i::STRING AS j FROM a");

		DatabaseMetaData md = conn.getMetaData();
		ResultSet rs;

		rs = md.getTableTypes();
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_TYPE"), "BASE TABLE");
		assertEquals(rs.getString(1), "BASE TABLE");

		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_TYPE"), "VIEW");
		assertEquals(rs.getString(1), "VIEW");

		assertFalse(rs.next());
		rs.close();

		rs = md.getCatalogs();
		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));

		assertFalse(rs.next());
		rs.close();

		rs = md.getSchemas(null, "ma%");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertNull(rs.getObject("TABLE_CATALOG"));
		assertEquals(rs.getString(1), "main");
		assertNull(rs.getObject(2));

		assertFalse(rs.next());
		rs.close();

		rs = md.getSchemas(null, "xxx");
		assertFalse(rs.next());
		rs.close();

		rs = md.getTables(null, null, "%", null);

		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "a");
		assertEquals(rs.getString(3), "a");
		assertEquals(rs.getString("TABLE_TYPE"), "BASE TABLE");
		assertEquals(rs.getString(4), "BASE TABLE");
		assertNull(rs.getObject("REMARKS"));
		assertNull(rs.getObject(5));
		assertNull(rs.getObject("TYPE_CAT"));
		assertNull(rs.getObject(6));
		assertNull(rs.getObject("TYPE_SCHEM"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("TYPE_NAME"));
		assertNull(rs.getObject(8));
		assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
		assertNull(rs.getObject(9));
		assertNull(rs.getObject("REF_GENERATION"));
		assertNull(rs.getObject(10));

		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "b");
		assertEquals(rs.getString(3), "b");
		assertEquals(rs.getString("TABLE_TYPE"), "VIEW");
		assertEquals(rs.getString(4), "VIEW");
		assertNull(rs.getObject("REMARKS"));
		assertNull(rs.getObject(5));
		assertNull(rs.getObject("TYPE_CAT"));
		assertNull(rs.getObject(6));
		assertNull(rs.getObject("TYPE_SCHEM"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("TYPE_NAME"));
		assertNull(rs.getObject(8));
		assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
		assertNull(rs.getObject(9));
		assertNull(rs.getObject("REF_GENERATION"));
		assertNull(rs.getObject(10));

		assertFalse(rs.next());
		rs.close();

		rs = md.getTables(null, "main", "a", null);

		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "a");
		assertEquals(rs.getString(3), "a");
		assertEquals(rs.getString("TABLE_TYPE"), "BASE TABLE");
		assertEquals(rs.getString(4), "BASE TABLE");
		assertNull(rs.getObject("REMARKS"));
		assertNull(rs.getObject(5));
		assertNull(rs.getObject("TYPE_CAT"));
		assertNull(rs.getObject(6));
		assertNull(rs.getObject("TYPE_SCHEM"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("TYPE_NAME"));
		assertNull(rs.getObject(8));
		assertNull(rs.getObject("SELF_REFERENCING_COL_NAME"));
		assertNull(rs.getObject(9));
		assertNull(rs.getObject("REF_GENERATION"));
		assertNull(rs.getObject(10));
		assertFalse(rs.next());

		rs.close();

		rs = md.getTables(null, "main", "xxx", null);
		assertFalse(rs.next());
		rs.close();

		rs = md.getColumns(null, null, "a", null);
		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "a");
		assertEquals(rs.getString(3), "a");
		assertEquals(rs.getString("COLUMN_NAME"), "i");
		assertEquals(rs.getString(4), "i");
		assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
		assertEquals(rs.getInt(5), Types.INTEGER);
		assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
		assertEquals(rs.getString(6), "INTEGER");
		assertNull(rs.getObject("COLUMN_SIZE"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("BUFFER_LENGTH"));
		assertNull(rs.getObject(8));

		// and so on but whatever

		assertFalse(rs.next());
		rs.close();

		rs = md.getColumns(null, "main", "a", "i");
		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "a");
		assertEquals(rs.getString(3), "a");
		assertEquals(rs.getString("COLUMN_NAME"), "i");
		assertEquals(rs.getString(4), "i");
		assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
		assertEquals(rs.getInt(5), Types.INTEGER);
		assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
		assertEquals(rs.getString(6), "INTEGER");
		assertNull(rs.getObject("COLUMN_SIZE"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("BUFFER_LENGTH"));
		assertNull(rs.getObject(8));

		assertFalse(rs.next());
		rs.close();

		// try with catalog as well
		rs = md.getColumns(conn.getCatalog(), "main", "a", "i");
		assertTrue(rs.next());
		assertNull(rs.getObject("TABLE_CAT"));
		assertNull(rs.getObject(1));
		assertEquals(rs.getString("TABLE_SCHEM"), "main");
		assertEquals(rs.getString(2), "main");
		assertEquals(rs.getString("TABLE_NAME"), "a");
		assertEquals(rs.getString(3), "a");
		assertEquals(rs.getString("COLUMN_NAME"), "i");
		assertEquals(rs.getString(4), "i");
		assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
		assertEquals(rs.getInt(5), Types.INTEGER);
		assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
		assertEquals(rs.getString(6), "INTEGER");
		assertNull(rs.getObject("COLUMN_SIZE"));
		assertNull(rs.getObject(7));
		assertNull(rs.getObject("BUFFER_LENGTH"));
		assertNull(rs.getObject(8));

		assertFalse(rs.next());
		rs.close();

		rs = md.getColumns(null, "xxx", "a", "i");
		assertFalse(rs.next());
		rs.close();

		rs = md.getColumns(null, "main", "xxx", "i");
		assertFalse(rs.next());
		rs.close();

		rs = md.getColumns(null, "main", "a", "xxx");
		assertFalse(rs.next());
		rs.close();

		conn.close();
	}

	public static void test_connect_wrong_url_bug848() throws Exception {
		Driver d = new DuckDBDriver();
		assertNull(d.connect("jdbc:h2:", null));
	}

	public static void test_parquet_reader() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM parquet_scan('data/parquet-testing/userdata1.parquet')");
		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 1000);
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_crash_autocommit_bug939() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		PreparedStatement stmt = conn.prepareStatement("CREATE TABLE ontime(flightdate DATE)");
		conn.setAutoCommit(false); // The is the key to getting the crash to happen.
		stmt.executeUpdate();
		stmt.close();
		conn.close();
	}

	public static void test_explain_bug958() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("EXPLAIN SELECT 42");
		assertTrue(rs.next());
		assertTrue(rs.getString(1) != null);
		assertTrue(rs.getString(2) != null);

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_appender_numbers() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		// int8, int4, int2, int1, float8, float4
		stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
		DuckDBAppender appender = conn.createAppender("main", "numbers");

		for (int i = 0; i < 50; i++) {
			appender.beginRow();
			appender.append(Long.MAX_VALUE - i);
			appender.append(Integer.MAX_VALUE - i);
			appender.append(Short.MAX_VALUE - i);
			appender.append(Byte.MAX_VALUE - i);
			appender.append(i);
			appender.append(i);
			appender.endRow();
		}
		appender.close();

		ResultSet rs = stmt.executeQuery("SELECT max(a), max(b), max(c), max(d), max(e), max(f) FROM numbers");
		assertFalse(rs.isClosed());
		assertTrue(rs.next());

		long resA = rs.getLong(1);
		assertEquals(resA, Long.MAX_VALUE);

		int resB = rs.getInt(2);
		assertEquals(resB, Integer.MAX_VALUE);

		short resC = rs.getShort(3);
		assertEquals(resC, Short.MAX_VALUE);

		byte resD = rs.getByte(4);
		assertEquals(resD, Byte.MAX_VALUE);

		double resE = rs.getDouble(5);
		assertEquals(resE, 49.0d);

		float resF = rs.getFloat(6);
		assertEquals(resF, 49.0f);

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_appender_int_string() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER, s VARCHAR)");
		DuckDBAppender appender = conn.createAppender("main", "data");

		for (int i = 0; i < 1000; i++) {
			appender.beginRow();
			appender.append(i);
			appender.append("str " + i);
			appender.endRow();
		}
		appender.close();

		ResultSet rs = stmt.executeQuery("SELECT max(a), min(s) FROM data");
		assertFalse(rs.isClosed());

		assertTrue(rs.next());
		int resA = rs.getInt(1);
		assertEquals(resA, 999);
		String resB = rs.getString(2);
		assertEquals(resB, "str 0");

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_appender_table_does_not_exist() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		try {
			@SuppressWarnings("unused")
			DuckDBAppender appender = conn.createAppender("main", "data");
			fail();
		} catch (SQLException e) {
		}

		stmt.close();
		conn.close();
	}

	public static void test_appender_table_deleted() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER)");
		DuckDBAppender appender = conn.createAppender("main", "data");

		appender.beginRow();
		appender.append(1);
		appender.endRow();

		stmt.execute("DROP TABLE data");

		appender.beginRow();
		appender.append(2);
		appender.endRow();

		try {
			appender.close();
			fail();
		} catch (SQLException e) {
		}

		stmt.close();
		conn.close();
	}

	public static void test_appender_append_too_many_columns() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER)");
		stmt.close();
		DuckDBAppender appender = conn.createAppender("main", "data");

		try {
			appender.beginRow();
			appender.append(1);
			appender.append(2);
			fail();
		} catch (SQLException e) {
		}

		conn.close();
	}

	public static void test_appender_append_too_few_columns() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
		stmt.close();
		DuckDBAppender appender = conn.createAppender("main", "data");

		try {
			appender.beginRow();
			appender.append(1);
			appender.endRow();
			fail();
		} catch (SQLException e) {
		}

		conn.close();
	}

	public static void test_appender_type_mismatch() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER)");
		DuckDBAppender appender = conn.createAppender("main", "data");

		try {
			appender.beginRow();
			appender.append("str");
			fail();
		} catch (SQLException e) {
		}

		stmt.close();
		conn.close();
	}

	public static void test_appender_null_integer() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a INTEGER)");

		DuckDBAppender appender = conn.createAppender("main", "data");

		appender.beginRow();
		appender.append(null);
		appender.endRow();
		appender.flush();
		appender.close();

		ResultSet results = stmt.executeQuery("SELECT * FROM data");
		assertTrue(results.next());
		// java.sql.ResultSet.getInt(int) returns 0 if the value is NULL
		assertEquals(0, results.getInt(1));
		assertTrue(results.wasNull());

		results.close();
		stmt.close();
		conn.close();
	}

	public static void test_appender_null_varchar() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		stmt.execute("CREATE TABLE data (a VARCHAR)");

		DuckDBAppender appender = conn.createAppender("main", "data");

		appender.beginRow();
		appender.append(null);
		appender.endRow();
		appender.flush();
		appender.close();

		ResultSet results = stmt.executeQuery("SELECT * FROM data");
		assertTrue(results.next());
		assertNull(results.getString(1));
		assertTrue(results.wasNull());

		results.close();
		stmt.close();
		conn.close();
	}

	public static void test_get_catalog() throws Exception {
		Connection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		ResultSet rs = conn.getMetaData().getCatalogs();
		assertTrue(rs.next());
		String catalog = rs.getString(1);
		assertFalse(rs.next());
		rs.close();
		assertEquals(conn.getCatalog(), catalog);
		conn.close();
	}

	public static void test_set_catalog() throws Exception {
		Connection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		conn.setCatalog("we do not have this feature yet, sorry"); // Should be no-op until implemented
		conn.close();
	}

	public static void test_get_table_types_bug1258() throws Exception {
		Connection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE a1 (i INTEGER)");
		stmt.execute("CREATE TABLE a2 (i INTEGER)");
		stmt.execute("CREATE TEMPORARY TABLE b (i INTEGER)");
		stmt.execute("CREATE VIEW c AS SELECT * FROM a1");
		stmt.close();

		ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a1");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a2");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "b");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "c");
		assertFalse(rs.next());
		rs.close();

		rs = conn.getMetaData().getTables(null, null, null, new String[] {});
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a1");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a2");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "b");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "c");
		assertFalse(rs.next());
		rs.close();

		rs = conn.getMetaData().getTables(null, null, null, new String[] { "BASE TABLE" });
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a1");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a2");
		assertFalse(rs.next());
		rs.close();

		rs = conn.getMetaData().getTables(null, null, null, new String[] { "BASE TABLE", "VIEW" });
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a1");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "a2");
		assertTrue(rs.next());
		assertEquals(rs.getString("TABLE_NAME"), "c");
		assertFalse(rs.next());
		rs.close();

		rs = conn.getMetaData().getTables(null, null, null, new String[] { "XXXX" });
		assertFalse(rs.next());
		rs.close();

		conn.close();
	}

	public static void test_utf_string_bug1271() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery("SELECT 'Mühleisen', '🦆', '🦄ྀི123456789'");
		assertEquals(rs.getMetaData().getColumnName(1), "'Mühleisen'");
		assertEquals(rs.getMetaData().getColumnName(2), "'🦆'");
		assertEquals(rs.getMetaData().getColumnName(3), "'🦄ྀི123456789'");

		assertTrue(rs.next());

		assertEquals(rs.getString(1), "Mühleisen");
		assertEquals(rs.getString(2), "🦆");
		assertEquals(rs.getString(3), "🦄ྀི123456789");

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_statement_creation_bug1268() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt;

		stmt = conn.createStatement();
		stmt.close();

		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.close();

		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
		stmt.close();

		PreparedStatement pstmt;
		pstmt = conn.prepareStatement("SELECT 42");
		pstmt.close();

		pstmt = conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		pstmt.close();

		pstmt = conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
		pstmt.close();

		conn.close();
	}

	private static String blob_to_string(Blob b) throws SQLException {
		return new String(b.getBytes(0, (int) b.length()), StandardCharsets.US_ASCII);
	}

	public static void test_blob_bug1090() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		String test_str1 = "asdf";
		String test_str2 = "asdxxxxxxxxxxxxxxf";

		ResultSet rs = stmt
				.executeQuery("SELECT '" + test_str1 + "'::BLOB a, NULL::BLOB b, '" + test_str2 + "'::BLOB c");
		assertTrue(rs.next());

		assertTrue(test_str1.equals(blob_to_string(rs.getBlob(1))));
		assertTrue(test_str1.equals(blob_to_string(rs.getBlob("a"))));

		assertTrue(test_str2.equals(blob_to_string(rs.getBlob("c"))));

		rs.getBlob("a");
		assertFalse(rs.wasNull());

		rs.getBlob("b");
		assertTrue(rs.wasNull());

		rs.close();
		stmt.close();
		conn.close();
	}

	public static void test_unsiged_integers() throws Exception {
		DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
		Statement stmt = conn.createStatement();

		ResultSet rs = stmt.executeQuery(
				"SELECT 201::utinyint uint8, 40001::usmallint uint16, 4000000001::uinteger uint32, 18446744073709551615::ubigint uint64");
		assertTrue(rs.next());

		assertEquals(rs.getShort("uint8"), Short.valueOf((short) 201));
		assertEquals(rs.getObject("uint8"), Short.valueOf((short) 201));
		assertEquals(rs.getInt("uint8"), Integer.valueOf((int) 201));

		assertEquals(rs.getInt("uint16"), Integer.valueOf((int) 40001));
		assertEquals(rs.getObject("uint16"), Integer.valueOf((int) 40001));
		assertEquals(rs.getLong("uint16"), Long.valueOf((long) 40001));

		assertEquals(rs.getLong("uint32"), Long.valueOf((long) 4000000001L));
		assertEquals(rs.getObject("uint32"), Long.valueOf((long) 4000000001L));

		assertEquals(rs.getObject("uint64"), new BigInteger("18446744073709551615"));

		rs.close();

		rs = stmt.executeQuery(
				"SELECT NULL::utinyint uint8, NULL::usmallint uint16, NULL::uinteger uint32, NULL::ubigint uint64");
		assertTrue(rs.next());

		rs.getObject(1);
		assertTrue(rs.wasNull());

		rs.getObject(2);
		assertTrue(rs.wasNull());

		rs.getObject(3);
		assertTrue(rs.wasNull());

		rs.getObject(4);
		assertTrue(rs.wasNull());

		stmt.close();
		conn.close();
	}

	public static void main(String[] args) throws Exception {
		// Woo I can do reflection too, take this, JUnit!
		Method[] methods = TestDuckDBJDBC.class.getMethods();
		for (Method m : methods) {
			if (m.getName().startsWith("test_")) {
				m.invoke(null);
			}
		}
		System.out.println("OK");
	}
}