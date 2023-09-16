package org.duckdb.test;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.duckdb.DuckDBNative;
import org.duckdb.DuckDBResultSet;
import org.duckdb.DuckDBResultSetMetaData;
import org.duckdb.DuckDBStruct;
import org.duckdb.DuckDBTimestamp;
import org.duckdb.JsonNode;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.util.concurrent.Future;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.OFFSET_SECONDS;
import static java.time.temporal.ChronoField.YEAR_OF_ERA;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public class TestDuckDBJDBC {

    private static void assertTrue(boolean val) throws Exception {
        assertTrue(val, null);
    }

    private static void assertTrue(boolean val, String message) throws Exception {
        if (!val) {
            throw new Exception(message);
        }
    }

    private static void assertFalse(boolean val) throws Exception {
        assertTrue(!val);
    }

    private static void assertEquals(Object actual, Object expected) throws Exception {
        Function<Object, String> getClass = (Object a) -> a == null ? "null" : a.getClass().toString();

        String message = String.format("\"%s\" (of %s) should equal \"%s\" (of %s)", actual, getClass.apply(actual),
                                       expected, getClass.apply(expected));
        assertTrue(Objects.equals(actual, expected), message);
    }

    private static void assertNotNull(Object a) throws Exception {
        assertFalse(a == null);
    }

    private static void assertNull(Object a) throws Exception {
        assertEquals(a, null);
    }

    private static void assertEquals(double a, double b, double epsilon) throws Exception {
        assertTrue(Math.abs(a - b) < epsilon);
    }

    private static void fail() throws Exception {
        fail(null);
    }

    private static void fail(String s) throws Exception {
        throw new Exception(s);
    }

    private static <T extends Throwable> String assertThrows(Thrower thrower, Class<T> exception) throws Exception {
        return assertThrows(exception, thrower).getMessage();
    }

    private static <T extends Throwable> Throwable assertThrows(Class<T> exception, Thrower thrower) throws Exception {
        try {
            thrower.run();
        } catch (Throwable e) {
            assertEquals(e.getClass(), exception);
            return e;
        }
        throw new Exception("Expected to throw " + exception.getName());
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

        assertThrows(() -> {
            ResultSet rs = stmt.executeQuery("SELECT");
            rs.next();
        }, SQLException.class);
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
        } catch (SQLException e) {
        }

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
        stmt.execute(
            "CREATE TYPE enum_long AS ENUM ('enum0' ,'enum1' ,'enum2' ,'enum3' ,'enum4' ,'enum5' ,'enum6'"
            +
            ",'enum7' ,'enum8' ,'enum9' ,'enum10' ,'enum11' ,'enum12' ,'enum13' ,'enum14' ,'enum15' ,'enum16' ,'enum17'"
            +
            ",'enum18' ,'enum19' ,'enum20' ,'enum21' ,'enum22' ,'enum23' ,'enum24' ,'enum25' ,'enum26' ,'enum27' ,'enum28'"
            +
            ",'enum29' ,'enum30' ,'enum31' ,'enum32' ,'enum33' ,'enum34' ,'enum35' ,'enum36' ,'enum37' ,'enum38' ,'enum39'"
            +
            ",'enum40' ,'enum41' ,'enum42' ,'enum43' ,'enum44' ,'enum45' ,'enum46' ,'enum47' ,'enum48' ,'enum49' ,'enum50'"
            +
            ",'enum51' ,'enum52' ,'enum53' ,'enum54' ,'enum55' ,'enum56' ,'enum57' ,'enum58' ,'enum59' ,'enum60' ,'enum61'"
            +
            ",'enum62' ,'enum63' ,'enum64' ,'enum65' ,'enum66' ,'enum67' ,'enum68' ,'enum69' ,'enum70' ,'enum71' ,'enum72'"
            +
            ",'enum73' ,'enum74' ,'enum75' ,'enum76' ,'enum77' ,'enum78' ,'enum79' ,'enum80' ,'enum81' ,'enum82' ,'enum83'"
            +
            ",'enum84' ,'enum85' ,'enum86' ,'enum87' ,'enum88' ,'enum89' ,'enum90' ,'enum91' ,'enum92' ,'enum93' ,'enum94'"
            +
            ",'enum95' ,'enum96' ,'enum97' ,'enum98' ,'enum99' ,'enum100' ,'enum101' ,'enum102' ,'enum103' ,'enum104' "
            +
            ",'enum105' ,'enum106' ,'enum107' ,'enum108' ,'enum109' ,'enum110' ,'enum111' ,'enum112' ,'enum113' ,'enum114'"
            +
            ",'enum115' ,'enum116' ,'enum117' ,'enum118' ,'enum119' ,'enum120' ,'enum121' ,'enum122' ,'enum123' ,'enum124'"
            +
            ",'enum125' ,'enum126' ,'enum127' ,'enum128' ,'enum129' ,'enum130' ,'enum131' ,'enum132' ,'enum133' ,'enum134'"
            +
            ",'enum135' ,'enum136' ,'enum137' ,'enum138' ,'enum139' ,'enum140' ,'enum141' ,'enum142' ,'enum143' ,'enum144'"
            +
            ",'enum145' ,'enum146' ,'enum147' ,'enum148' ,'enum149' ,'enum150' ,'enum151' ,'enum152' ,'enum153' ,'enum154'"
            +
            ",'enum155' ,'enum156' ,'enum157' ,'enum158' ,'enum159' ,'enum160' ,'enum161' ,'enum162' ,'enum163' ,'enum164'"
            +
            ",'enum165' ,'enum166' ,'enum167' ,'enum168' ,'enum169' ,'enum170' ,'enum171' ,'enum172' ,'enum173' ,'enum174'"
            +
            ",'enum175' ,'enum176' ,'enum177' ,'enum178' ,'enum179' ,'enum180' ,'enum181' ,'enum182' ,'enum183' ,'enum184'"
            +
            ",'enum185' ,'enum186' ,'enum187' ,'enum188' ,'enum189' ,'enum190' ,'enum191' ,'enum192' ,'enum193' ,'enum194'"
            +
            ",'enum195' ,'enum196' ,'enum197' ,'enum198' ,'enum199' ,'enum200' ,'enum201' ,'enum202' ,'enum203' ,'enum204'"
            +
            ",'enum205' ,'enum206' ,'enum207' ,'enum208' ,'enum209' ,'enum210' ,'enum211' ,'enum212' ,'enum213' ,'enum214'"
            +
            ",'enum215' ,'enum216' ,'enum217' ,'enum218' ,'enum219' ,'enum220' ,'enum221' ,'enum222' ,'enum223' ,'enum224'"
            +
            ",'enum225' ,'enum226' ,'enum227' ,'enum228' ,'enum229' ,'enum230' ,'enum231' ,'enum232' ,'enum233' ,'enum234'"
            +
            ",'enum235' ,'enum236' ,'enum237' ,'enum238' ,'enum239' ,'enum240' ,'enum241' ,'enum242' ,'enum243' ,'enum244'"
            +
            ",'enum245' ,'enum246' ,'enum247' ,'enum248' ,'enum249' ,'enum250' ,'enum251' ,'enum252' ,'enum253' ,'enum254'"
            +
            ",'enum255' ,'enum256' ,'enum257' ,'enum258' ,'enum259' ,'enum260' ,'enum261' ,'enum262' ,'enum263' ,'enum264'"
            +
            ",'enum265' ,'enum266' ,'enum267' ,'enum268' ,'enum269' ,'enum270' ,'enum271' ,'enum272' ,'enum273' ,'enum274'"
            +
            ",'enum275' ,'enum276' ,'enum277' ,'enum278' ,'enum279' ,'enum280' ,'enum281' ,'enum282' ,'enum283' ,'enum284'"
            +
            ",'enum285' ,'enum286' ,'enum287' ,'enum288' ,'enum289' ,'enum290' ,'enum291' ,'enum292' ,'enum293' ,'enum294'"
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

    public static void test_timestamp_ms() throws Exception {
        String expectedString = "2022-08-17 12:11:10.999";
        String sql = "SELECT '2022-08-17T12:11:10.999'::TIMESTAMP_MS as ts_ms";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_MS");
    }

    public static void test_timestamp_ns() throws Exception {
        String expectedString = "2022-08-17 12:11:10.999999";
        String sql = "SELECT '2022-08-17T12:11:10.999999999'::TIMESTAMP_NS as ts_ns";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_NS");
    }

    public static void test_timestamp_s() throws Exception {
        String expectedString = "2022-08-17 12:11:10";
        String sql = "SELECT '2022-08-17T12:11:10'::TIMESTAMP_S as ts_s";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_S");
    }

    private static void assert_timestamp_match(String fetchSql, String expectedString, String expectedTypeName)
        throws Exception {
        String originalTzProperty = System.getProperty("user.timezone");
        TimeZone originalTz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            System.setProperty("user.timezone", "UTC");
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(fetchSql);
            assertTrue(rs.next());
            Timestamp actual = rs.getTimestamp(1);

            Timestamp expected = Timestamp.valueOf(expectedString);

            assertEquals(expected.getTime(), actual.getTime());
            assertEquals(expected.getNanos(), actual.getNanos());

            //	Verify calendar variants
            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles"), Locale.US);
            Timestamp actual_cal = rs.getTimestamp(1, cal);
            assertEquals(expected.getTime(), actual_cal.getTime());
            assertEquals(expected.getNanos(), actual_cal.getNanos());

            assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
            assertEquals(expectedTypeName, rs.getMetaData().getColumnTypeName(1));

            rs.close();
            stmt.close();
            conn.close();
        } finally {
            TimeZone.setDefault(originalTz);
            System.setProperty("user.timezone", originalTzProperty);
        }
    }

    public static void test_timestamp_tz() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet rs;

        stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ)");
        stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10+02')");
        stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:10Z')");

        PreparedStatement ps = conn.prepareStatement("INSERT INTO T (id, t1) VALUES (?, ?)");

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
        assertTrue(((OffsetDateTime) rs.getObject(2)).isEqual(odt2Rounded));

        // Metadata tests
        assertEquals(
            Types.TIMESTAMP_WITH_TIMEZONE,
            (meta.unwrap(DuckDBResultSetMetaData.class).type_to_int(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)));
        assertTrue(OffsetDateTime.class.getName().equals(meta.getColumnClassName(2)));

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_timestamp_as_long() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet rs;

        stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMP)");
        stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10')");
        stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:11')");

        rs = stmt.executeQuery("SELECT * FROM t ORDER BY id");
        rs.next();
        assertEquals(rs.getLong(2), 1641039070000000L);
        rs.next();
        assertEquals(rs.getLong(2), 1641039071000000L);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_timestamptz_as_long() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet rs;

        stmt.execute("SET CALENDAR='gregorian'");
        stmt.execute("SET TIMEZONE='America/Los_Angeles'");
        stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ)");
        stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10Z')");
        stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:11Z')");

        rs = stmt.executeQuery("SELECT * FROM t ORDER BY id");
        rs.next();
        assertEquals(rs.getLong(2), 1641039070000000L);
        rs.next();
        assertEquals(rs.getLong(2), 1641039071000000L);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_consecutive_timestamps() throws Exception {
        long expected = 986860800000L;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                     "select range from range(TIMESTAMP '2001-04-10', TIMESTAMP '2001-04-11', INTERVAL 30 MINUTE)")) {
                while (rs.next()) {
                    Timestamp actual = rs.getTimestamp(1, Calendar.getInstance());
                    assertEquals(expected, actual.getTime());
                    expected += 30 * 60 * 1_000;
                }
            }
        }
    }

    public static void test_throw_wrong_datatype() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs;

        stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ, t2 TIMESTAMP)");
        stmt.execute("INSERT INTO t (id, t1, t2) VALUES (1, '2022-01-01T12:11:10+02', '2022-01-01T12:11:10')");

        rs = stmt.executeQuery("SELECT * FROM t");
        rs.next();

        try {
            rs.getShort(2);
            fail();
        } catch (IllegalArgumentException e) {
        }

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_list_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT generate_series(2) as list");) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "list");
            assertEquals(meta.getColumnTypeName(1), "BIGINT[]");
            assertEquals(meta.getColumnType(1), Types.ARRAY);
        }
    }

    public static void test_struct_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT {'i': 42, 'j': 'a'} as struct")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "struct");
            assertEquals(meta.getColumnTypeName(1), "STRUCT(i INTEGER, j VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.JAVA_OBJECT);
        }
    }

    public static void test_map_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT map([1,2],['a','b']) as map")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "map");
            assertEquals(meta.getColumnTypeName(1), "MAP(INTEGER, VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.JAVA_OBJECT);
        }
    }

    public static void test_union_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT union_value(str := 'three') as union")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "union");
            assertEquals(meta.getColumnTypeName(1), "UNION(str VARCHAR)");
            assertEquals(meta.getColumnType(1), Types.JAVA_OBJECT);
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
        Connection conn2 = conn.unwrap(DuckDBConnection.class).duplicate();
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

        assertEquals(assertThrows(() -> rs.getObject(1), SQLException.class), "No row in context");

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

        // Generate tests without database
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
        ResultSet rs = stmt.executeQuery("SELECT * FROM a");
        assertTrue(rs.next());
        assertEquals(rs.getObject("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));
        assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));

        rs.close();
        stmt.close();

        PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?");
        ps.setTimestamp(1, Timestamp.valueOf("2005-11-02 07:59:58"));
        ResultSet rs2 = ps.executeQuery();
        assertTrue(rs2.next());
        assertEquals(rs2.getInt(1), 1);
        rs2.close();
        ps.close();

        ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?");
        ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"));
        ResultSet rs3 = ps.executeQuery();
        assertTrue(rs3.next());
        assertEquals(rs3.getInt(1), 1);
        rs3.close();
        ps.close();

        ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?");
        ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"), Types.TIMESTAMP);
        ResultSet rs4 = ps.executeQuery();
        assertTrue(rs4.next());
        assertEquals(rs4.getInt(1), 1);
        rs4.close();
        ps.close();

        Statement stmt2 = conn.createStatement();
        stmt2.execute("INSERT INTO a (ts) VALUES ('1905-11-02 07:59:58.12345')");
        ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?");
        ps.setTimestamp(1, Timestamp.valueOf("1905-11-02 07:59:58.12345"));
        ResultSet rs5 = ps.executeQuery();
        assertTrue(rs5.next());
        assertEquals(rs5.getInt(1), 1);
        rs5.close();
        ps.close();

        ps = conn.prepareStatement("SELECT ts FROM a WHERE ts = ?");
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

        LocalDateTime ldt = LocalDateTime.of(2021, 1, 18, 21, 20, 7);

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
        stmt.execute(
            "INSERT INTO b VALUES ('varchary', true, 6, 42, 666, 42.666, 666.42,"
            +
            " '1970-01-02', '01:00:34', '1970-01-03 03:42:23', 42.2, 1.23456789, 987654321012345.6, 111112222233333.44444, "
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
        assertEquals(rs.getObject(10, LocalDateTime.class), LocalDateTime.of(1970, 1, 3, 3, 42, 23));
        assertEquals(rs.getBigDecimal(11), rs.getObject(11, BigDecimal.class));
        assertEquals(rs.getBigDecimal(12), rs.getObject(12, BigDecimal.class));
        assertEquals(rs.getBigDecimal(13), rs.getObject(13, BigDecimal.class));
        assertEquals(rs.getBigDecimal(14), rs.getObject(14, BigDecimal.class));

        // Missing implementations, should never reach assertTrue(false)
        try {
            rs.getObject(11, Integer.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(12, Integer.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(13, Integer.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(14, Long.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(15, BigInteger.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(16, BigInteger.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        try {
            rs.getObject(16, Blob.class);
            assertTrue(false);
        } catch (SQLException e) {
        }

        rs.close();
        ps.close();
        stmt.close();
        conn.close();
    }

    public static void test_multiple_statements_execution() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("CREATE TABLE integers(i integer);\n"
                                         + "insert into integers select * from range(10);"
                                         + "select * from integers;");
        int i = 0;
        while (rs.next()) {
            assertEquals(rs.getInt("i"), i);
            i++;
        }
        assertEquals(i, 10);
    }

    public static void test_multiple_statements_exception() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        boolean succ = false;
        try {
            stmt.executeQuery("CREATE TABLE integers(i integer, i boolean);\n"
                              + "CREATE TABLE integers2(i integer);\n"
                              + "insert into integers2 select * from range(10);\n"
                              + "select * from integers2;");
            succ = true;
        } catch (Exception ex) {
            assertFalse(succ);
        }
    }

    public static void test_bigdecimal() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        stmt.execute(
            "CREATE TABLE q (id DECIMAL(3,0), dec16 DECIMAL(4,1), dec32 DECIMAL(9,4), dec64 DECIMAL(18,7), dec128 DECIMAL(38,10))");

        PreparedStatement ps1 =
            conn.prepareStatement("INSERT INTO q (id, dec16, dec32, dec64, dec128) VALUES (?, ?, ?, ?, ?)");
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
        while (rs.next()) {
            assertEquals(rs.getBigDecimal(1), rs.getObject(1, BigDecimal.class));
            assertEquals(rs.getBigDecimal(2), rs.getObject(2, BigDecimal.class));
            assertEquals(rs.getBigDecimal(3), rs.getObject(3, BigDecimal.class));
            assertEquals(rs.getBigDecimal(4), rs.getObject(4, BigDecimal.class));
            assertEquals(rs.getBigDecimal(5), rs.getObject(5, BigDecimal.class));
        }

        rs.close();

        ResultSet rs2 = ps.executeQuery();
        DuckDBResultSetMetaData meta = rs2.getMetaData().unwrap(DuckDBResultSetMetaData.class);
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
        assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(1)));
        assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(2)));
        assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(3)));
        assertTrue(BigDecimal.class.getName().equals(meta.getColumnClassName(4)));

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

        for (long i = 134234533L; i < 13423453300L; i = i + 735127) {
            ts.setTime(i);
            stmt.execute("INSERT INTO a (ts) VALUES ('" + ts + "')");
        }

        stmt.close();

        for (long i = 134234533L; i < 13423453300L; i = i + 735127) {
            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?");
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
        // Create the table
        stmt.execute("CREATE TABLE q (id DECIMAL(4,0),dec32 DECIMAL(9,4),dec64 DECIMAL(18,7),dec128 DECIMAL(38,10))");
        stmt.close();

        // Create the INSERT prepared statement we will use
        PreparedStatement ps1 = conn.prepareStatement("INSERT INTO q (id, dec32, dec64, dec128) VALUES (?, ?, ?, ?)");

        // Create the Java decimals we will be inserting
        BigDecimal id_org = new BigDecimal("1");
        BigDecimal dec32_org = new BigDecimal("99999.9999");
        BigDecimal dec64_org = new BigDecimal("99999999999.9999999");
        BigDecimal dec128_org = new BigDecimal("9999999999999999999999999999.9999999999");

        // Insert the initial values
        ps1.setObject(1, id_org);
        ps1.setObject(2, dec32_org);
        ps1.setObject(3, dec64_org);
        ps1.setObject(4, dec128_org);
        // This does not have a result set
        assertFalse(ps1.execute());

        // Create the SELECT prepared statement we will use
        PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM q WHERE id = ?");
        BigDecimal multiplicant = new BigDecimal("0.987");

        BigDecimal dec32;
        BigDecimal dec64;
        BigDecimal dec128;

        ResultSet select_result;

        for (int i = 2; i < 10000; i++) {
            ps2.setObject(1, new BigDecimal(i - 1));

            // Verify that both the 'getObject' and the 'getBigDecimal' methods return the same value\

            select_result = ps2.executeQuery();
            assertTrue(select_result.next());
            dec32 = select_result.getObject(2, BigDecimal.class);
            dec64 = select_result.getObject(3, BigDecimal.class);
            dec128 = select_result.getObject(4, BigDecimal.class);
            assertEquals(dec32_org, dec32);
            assertEquals(dec64_org, dec64);
            assertEquals(dec128_org, dec128);
            select_result.close();

            select_result = ps2.executeQuery();
            assertTrue(select_result.next());
            dec32 = select_result.getBigDecimal(2);
            dec64 = select_result.getBigDecimal(3);
            dec128 = select_result.getBigDecimal(4);
            assertEquals(dec32_org, dec32);
            assertEquals(dec64_org, dec64);
            assertEquals(dec128_org, dec128);
            select_result.close();

            // Apply the modification for the next iteration

            dec32_org = dec32_org.multiply(multiplicant).setScale(4, java.math.RoundingMode.HALF_EVEN);
            dec64_org = dec64_org.multiply(multiplicant).setScale(7, java.math.RoundingMode.HALF_EVEN);
            dec128_org = dec128_org.multiply(multiplicant).setScale(10, java.math.RoundingMode.HALF_EVEN);

            ps1.clearParameters();
            ps1.setObject(1, new BigDecimal(i));
            ps1.setObject(2, dec32_org);
            ps1.setObject(3, dec64_org);
            ps1.setObject(4, dec128_org);
            assertFalse(ps1.execute());

            ps2.clearParameters();
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
                             + "CREATE TABLE t1(c0 DOUBLE, PRIMARY KEY(c0));\n"
                             + "INSERT INTO t0(c0) VALUES (0), (0), (0), (0);\n"
                             + "INSERT INTO t0(c0) VALUES (NULL), (NULL);\n"
                             + "INSERT INTO t1(c0) VALUES (0), (1);\n"
                             + "\n"
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
        ps.setString(8, "eight eight\n\t");

        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getBoolean(1), false);
        assertEquals(rs.getByte(2), (byte) 82);
        assertEquals(rs.getShort(3), (short) 83);
        assertEquals(rs.getInt(4), 84);
        assertEquals(rs.getLong(5), (long) 85);
        assertEquals(rs.getFloat(6), 8.6, 0.001);
        assertEquals(rs.getDouble(7), 8.7, 0.001);
        assertEquals(rs.getString(8), "eight eight\n\t");
        rs.close();

        ps.setObject(1, false);
        ps.setObject(2, (byte) 82);
        ps.setObject(3, (short) 83);
        ps.setObject(4, 84);
        ps.setObject(5, (long) 85);
        ps.setObject(6, (float) 8.6);
        ps.setObject(7, (double) 8.7);
        ps.setObject(8, "𫝼🔥😜䭔🟢");

        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getBoolean(1), false);
        assertEquals(rs.getByte(2), (byte) 82);
        assertEquals(rs.getShort(3), (short) 83);
        assertEquals(rs.getInt(4), 84);
        assertEquals(rs.getLong(5), (long) 85);
        assertEquals(rs.getFloat(6), 8.6, 0.001);
        assertEquals(rs.getDouble(7), 8.7, 0.001);
        assertEquals(rs.getString(8), "𫝼🔥😜䭔🟢");

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

        conn.createStatement().executeUpdate(
            "create table ctstable1 (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID))");
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
            String newName = "xx"
                             + "-" + i;
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
        Files.deleteIfExists(database_file);

        String jdbc_url = "jdbc:duckdb:" + database_file;
        Properties ro_prop = new Properties();
        ro_prop.setProperty("duckdb.read_only", "true");

        Connection conn_rw = DriverManager.getConnection(jdbc_url);
        assertFalse(conn_rw.isReadOnly());
        assertFalse(conn_rw.getMetaData().isReadOnly());
        Statement stmt = conn_rw.createStatement();
        stmt.execute("CREATE TABLE test (i INTEGER)");
        stmt.execute("INSERT INTO test VALUES (42)");
        stmt.close();

        // Verify we can open additional write connections
        // Using the Driver
        try (Connection conn = DriverManager.getConnection(jdbc_url); Statement stmt1 = conn.createStatement();
             ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
            rs1.next();
            assertEquals(rs1.getInt(1), 42);
        }
        // Using the direct API
        try (Connection conn = conn_rw.unwrap(DuckDBConnection.class).duplicate();
             Statement stmt1 = conn.createStatement(); ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
            rs1.next();
            assertEquals(rs1.getInt(1), 42);
        }

        // At this time, mixing read and write connections on Windows doesn't work
        // Read-only when we already have a read-write
        //		try (Connection conn = DriverManager.getConnection(jdbc_url, ro_prop);
        //				 Statement stmt1 = conn.createStatement();
        //				 ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
        //			rs1.next();
        //			assertEquals(rs1.getInt(1), 42);
        //		}

        conn_rw.close();

        try (Statement ignored = conn_rw.createStatement()) {
            fail("Connection was already closed; shouldn't be able to create a statement");
        } catch (SQLException e) {
        }

        try (Connection ignored = conn_rw.unwrap(DuckDBConnection.class).duplicate()) {
            fail("Connection was already closed; shouldn't be able to duplicate");
        } catch (SQLException e) {
        }

        // // we can create two parallel read only connections and query them, too
        try (Connection conn_ro1 = DriverManager.getConnection(jdbc_url, ro_prop);
             Connection conn_ro2 = DriverManager.getConnection(jdbc_url, ro_prop)) {

            assertTrue(conn_ro1.isReadOnly());
            assertTrue(conn_ro1.getMetaData().isReadOnly());
            assertTrue(conn_ro2.isReadOnly());
            assertTrue(conn_ro2.getMetaData().isReadOnly());

            try (Statement stmt1 = conn_ro1.createStatement();
                 ResultSet rs1 = stmt1.executeQuery("SELECT * FROM test")) {
                rs1.next();
                assertEquals(rs1.getInt(1), 42);
            }

            try (Statement stmt2 = conn_ro2.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT * FROM test")) {
                rs2.next();
                assertEquals(rs2.getInt(1), 42);
            }
        }
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

    public static void test_temporal_types() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(
            "SELECT '2019-11-26 21:11:00'::timestamp ts, '2019-11-26'::date dt, interval '5 days' iv, '21:11:00'::time te");
        assertTrue(rs.next());
        assertEquals(rs.getObject("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));
        assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2019-11-26 21:11:00"));

        assertEquals(rs.getObject("dt"), LocalDate.parse("2019-11-26"));
        assertEquals(rs.getDate("dt"), Date.valueOf("2019-11-26"));

        assertEquals(rs.getObject("iv"), "5 days");

        assertEquals(rs.getObject("te"), LocalTime.parse("21:11:00"));
        assertEquals(rs.getTime("te"), Time.valueOf("21:11:00"));

        assertFalse(rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_calendar_types() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        //	Nail down the location for test portability.
        Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles"), Locale.US);

        ResultSet rs = stmt.executeQuery(
            "SELECT '2019-11-26 21:11:43.123456'::timestamp ts, '2019-11-26'::date dt, '21:11:00'::time te");
        assertTrue(rs.next());
        assertEquals(rs.getTimestamp("ts", cal), Timestamp.from(Instant.ofEpochSecond(1574802703, 123456000)));

        assertEquals(rs.getDate("dt", cal), Date.valueOf("2019-11-26"));

        assertEquals(rs.getTime("te", cal), Time.valueOf("21:11:00"));

        assertFalse(rs.next());
        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_temporal_nulls() throws Exception {
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

        rs = md.getCatalogs();
        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        rs.close();

        rs = md.getSchemas(null, "ma%");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertTrue(rs.getObject("TABLE_CATALOG") != null);
        assertEquals(rs.getString(1), DuckDBConnection.DEFAULT_SCHEMA);
        rs.close();

        rs = md.getSchemas(null, "xxx");
        assertFalse(rs.next());
        rs.close();

        rs = md.getTables(null, null, "%", null);

        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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
        assertTrue(rs.getObject("TABLE_CAT") != null);
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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

        rs = md.getTables(null, DuckDBConnection.DEFAULT_SCHEMA, "a", null);

        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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

        rs.close();

        rs = md.getTables(null, DuckDBConnection.DEFAULT_SCHEMA, "xxx", null);
        assertFalse(rs.next());
        rs.close();

        rs = md.getColumns(null, null, "a", null);
        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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

        rs.close();

        rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "a", "i");
        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        ;
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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

        rs.close();

        // try with catalog as well
        rs = md.getColumns(conn.getCatalog(), DuckDBConnection.DEFAULT_SCHEMA, "a", "i");
        assertTrue(rs.next());
        assertTrue(rs.getObject("TABLE_CAT") != null);
        assertEquals(rs.getString("TABLE_SCHEM"), DuckDBConnection.DEFAULT_SCHEMA);
        assertEquals(rs.getString(2), DuckDBConnection.DEFAULT_SCHEMA);
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

        rs.close();

        rs = md.getColumns(null, "xxx", "a", "i");
        assertFalse(rs.next());
        rs.close();

        rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "xxx", "i");
        assertFalse(rs.next());
        rs.close();

        rs = md.getColumns(null, DuckDBConnection.DEFAULT_SCHEMA, "a", "xxx");
        assertFalse(rs.next());
        rs.close();

        conn.close();
    }

    public static void test_time_tz() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement s = conn.createStatement()) {
            s.executeUpdate("create table t (i time with time zone)");
            try (ResultSet rs = conn.getMetaData().getColumns(null, "%", "t", "i");) {
                rs.next();

                assertEquals(rs.getString("TYPE_NAME"), "TIME WITH TIME ZONE");
                assertEquals(rs.getInt("DATA_TYPE"), Types.JAVA_OBJECT);
            }

            s.execute("INSERT INTO t VALUES ('01:01:00'), ('01:02:03+12:30:45'), ('04:05:06-03:10'), ('07:08:09+20');");
            try (ResultSet rs = s.executeQuery("SELECT * FROM t")) {
                rs.next();
                assertEquals(rs.getObject(1), OffsetTime.of(LocalTime.of(1, 1), ZoneOffset.UTC));
                rs.next();
                assertEquals(rs.getObject(1),
                             OffsetTime.of(LocalTime.of(1, 2, 3), ZoneOffset.ofHoursMinutesSeconds(12, 30, 45)));
                rs.next();
                assertEquals(rs.getObject(1),
                             OffsetTime.of(LocalTime.of(4, 5, 6), ZoneOffset.ofHoursMinutesSeconds(-3, -10, 0)));
                rs.next();
                assertEquals(rs.getObject(1), OffsetTime.of(LocalTime.of(7, 8, 9), ZoneOffset.UTC));
            }
        }
    }

    public static void test_get_tables_with_current_catalog() throws Exception {
        ResultSet resultSet = null;
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        final String currentCatalog = conn.getCatalog();
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE T1(ID INT)");
        // verify that the catalog argument is supported and does not throw
        try {
            resultSet = databaseMetaData.getTables(currentCatalog, null, "%", null);
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("Actual catalog argument is not supported"));
        }
        assertTrue(resultSet.next(), "getTables should return exactly 1 table");
        final String returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            currentCatalog.equals(returnedCatalog),
            String.format("Returned catalog %s should equal current catalog %s", returnedCatalog, currentCatalog));
        assertTrue(resultSet.next() == false, "getTables should return exactly 1 table");

        resultSet.close();
    }

    public static void test_get_tables_with_attached_catalog() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        final String currentCatalog = conn.getCatalog();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        Statement statement = conn.createStatement();

        // create one table in the current catalog
        final String TABLE_NAME1 = "T1";
        statement.execute(String.format("CREATE TABLE %s(ID INT)", TABLE_NAME1));

        // create one table in an attached catalog
        String returnedCatalog, returnedTableName;
        ResultSet resultSet = null;
        final String ATTACHED_CATALOG = "ATTACHED_CATALOG";
        final String TABLE_NAME2 = "T2";
        statement.execute(String.format("ATTACH '' AS \"%s\"", ATTACHED_CATALOG));
        statement.execute(String.format("CREATE TABLE %s.%s(ID INT)", ATTACHED_CATALOG, TABLE_NAME2));

        // test if getTables can get tables from the remote catalog.
        resultSet = databaseMetaData.getTables(ATTACHED_CATALOG, null, "%", null);
        assertTrue(resultSet.next(), "getTables should return exactly 1 table");
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            ATTACHED_CATALOG.equals(returnedCatalog),
            String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog, ATTACHED_CATALOG));
        assertTrue(resultSet.next() == false, "getTables should return exactly 1 table");
        resultSet.close();

        // test if getTables with null catalog returns all tables.
        resultSet = databaseMetaData.getTables(null, null, "%", null);

        assertTrue(resultSet.next(), "getTables should return 2 tables, got 0");
        // first table should be ATTACHED_CATALOG.T2
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            ATTACHED_CATALOG.equals(returnedCatalog),
            String.format("Returned catalog %s should equal attached catalog %s", returnedCatalog, ATTACHED_CATALOG));
        returnedTableName = resultSet.getString("TABLE_NAME");
        assertTrue(TABLE_NAME2.equals(returnedTableName),
                   String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME2));

        assertTrue(resultSet.next(), "getTables should return 2 tables, got 1");
        // second table should be <current catalog>.T1
        returnedCatalog = resultSet.getString("TABLE_CAT");
        assertTrue(
            currentCatalog.equals(returnedCatalog),
            String.format("Returned catalog %s should equal current catalog %s", returnedCatalog, currentCatalog));
        returnedTableName = resultSet.getString("TABLE_NAME");
        assertTrue(TABLE_NAME1.equals(returnedTableName),
                   String.format("Returned table %s should equal %s", returnedTableName, TABLE_NAME1));

        assertTrue(resultSet.next() == false, "getTables should return 2 tables, got > 2");
        resultSet.close();
        statement.close();
        conn.close();
    }

    public static void test_get_tables_param_binding_for_table_types() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet rs = databaseMetaData.getTables(null, null, null,
                                                  new String[] {"') UNION ALL "
                                                                + "SELECT"
                                                                + " 'fake catalog'"
                                                                + ", ?"
                                                                + ", ?"
                                                                + ", 'fake table type'"
                                                                + ", 'fake remarks'"
                                                                + ", 'fake type cat'"
                                                                + ", 'fake type schem'"
                                                                + ", 'fake type name'"
                                                                + ", 'fake self referencing col name'"
                                                                + ", 'fake ref generation' -- "});
        assertFalse(rs.next());
        rs.close();
    }

    public static void test_get_table_types() throws Exception {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        List<String> tableTypesList = new ArrayList<>(asList(tableTypesArray));
        tableTypesList.sort(Comparator.naturalOrder());

        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet rs = databaseMetaData.getTableTypes();

        for (int i = 0; i < tableTypesArray.length; i++) {
            assertTrue(rs.next(), "Expected a row from table types resultset");
            String tableTypeFromResultSet = rs.getString("TABLE_TYPE");
            String tableTypeFromList = tableTypesList.get(i);
            assertTrue(tableTypeFromList.equals(tableTypeFromResultSet),
                       "Error in tableTypes at row " + (i + 1) + ": "
                           + "value from list " + tableTypeFromList + " should equal "
                           + "value from resultset " + tableTypeFromResultSet);
        }
    }

    public static void test_get_schemas_with_params() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        String inputCatalog = conn.getCatalog();
        String inputSchema = conn.getSchema();
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet resultSet = null;

        // catalog equal to current_catalog, schema null
        try {
            resultSet = databaseMetaData.getSchemas(inputCatalog, null);
            assertTrue(resultSet.next(), "Expected at least exactly 1 row, got 0");
            do {
                String outputCatalog = resultSet.getString("TABLE_CATALOG");
                assertTrue(inputCatalog.equals(outputCatalog),
                           "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                               inputCatalog);
            } while (resultSet.next());
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }

        // catalog equal to current_catalog, schema '%'
        ResultSet resultSetWithNullSchema = null;
        try {
            resultSet = databaseMetaData.getSchemas(inputCatalog, "%");
            resultSetWithNullSchema = databaseMetaData.getSchemas(inputCatalog, null);
            assertTrue(resultSet.next(), "Expected at least exactly 1 row, got 0");
            assertTrue(resultSetWithNullSchema.next(), "Expected at least exactly 1 row, got 0");
            do {
                String outputCatalog;
                outputCatalog = resultSet.getString("TABLE_CATALOG");
                assertTrue(inputCatalog.equals(outputCatalog),
                           "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                               inputCatalog);
                outputCatalog = resultSetWithNullSchema.getString("TABLE_CATALOG");
                assertTrue(inputCatalog.equals(outputCatalog),
                           "The catalog " + outputCatalog + " from getSchemas should equal the argument catalog " +
                               inputCatalog);
                String schema1 = resultSet.getString("TABLE_SCHEMA");
                String schema2 = resultSetWithNullSchema.getString("TABLE_SCHEMA");
                assertTrue(schema1.equals(schema2), "schema " + schema1 + " from getSchemas with % should equal " +
                                                        schema2 + " from getSchemas with null");
            } while (resultSet.next() && resultSetWithNullSchema.next());
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }

        // empty catalog
        try {
            resultSet = databaseMetaData.getSchemas("", null);
            assertTrue(resultSet.next() == false, "Expected 0 schemas, got > 0");
        } catch (SQLException ex) {
            assertFalse(ex.getMessage().startsWith("catalog argument is not supported"));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            conn.close();
        }
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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        // int8, int4, int2, int1, float8, float4
        stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "numbers");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER, s VARCHAR)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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

    public static void test_appender_string_with_emoji() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (str_value VARCHAR(10))");
        String expectedValue = "䭔\uD86D\uDF7C🔥\uD83D\uDE1C";
        try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
            appender.beginRow();
            appender.append(expectedValue);
            appender.endRow();
        }

        ResultSet rs = stmt.executeQuery("SELECT str_value FROM data");
        assertFalse(rs.isClosed());
        assertTrue(rs.next());

        String appendedValue = rs.getString(1);
        assertEquals(appendedValue, expectedValue);

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_appender_table_does_not_exist() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        try {
            @SuppressWarnings("unused")
            DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");
            fail();
        } catch (SQLException e) {
        }

        stmt.close();
        conn.close();
    }

    public static void test_appender_table_deleted() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        stmt.close();
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
        stmt.close();
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");
        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a INTEGER)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE data (a VARCHAR)");

        DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data");

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
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        ResultSet rs = conn.getMetaData().getCatalogs();
        HashSet<String> set = new HashSet<String>();
        while (rs.next()) {
            set.add(rs.getString(1));
        }
        assertTrue(!set.isEmpty());
        rs.close();
        assertTrue(set.contains(conn.getCatalog()));
        conn.close();
    }

    public static void test_set_catalog() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {

            assertThrows(() -> conn.setCatalog("other"), SQLException.class);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH ':memory:' AS other;");
            }

            conn.setCatalog("other");
            assertEquals(conn.getCatalog(), "other");
        }
    }

    public static void test_get_table_types_bug1258() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
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

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE"});
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"BASE TABLE", "VIEW"});
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a1");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "a2");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_NAME"), "c");
        assertFalse(rs.next());
        rs.close();

        rs = conn.getMetaData().getTables(null, null, null, new String[] {"XXXX"});
        assertFalse(rs.next());
        rs.close();

        conn.close();
    }

    public static void test_utf_string_bug1271() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
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
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        Statement stmt = conn.createStatement();

        String test_str1 = "asdf";
        String test_str2 = "asdxxxxxxxxxxxxxxf";

        ResultSet rs =
            stmt.executeQuery("SELECT '" + test_str1 + "'::BLOB a, NULL::BLOB b, '" + test_str2 + "'::BLOB c");
        assertTrue(rs.next());

        assertTrue(test_str1.equals(blob_to_string(rs.getBlob(1))));
        assertTrue(test_str1.equals(blob_to_string(rs.getBlob("a"))));

        assertTrue(test_str2.equals(blob_to_string(rs.getBlob("c"))));

        rs.getBlob("a");
        assertFalse(rs.wasNull());

        rs.getBlob("b");
        assertTrue(rs.wasNull());

        assertEquals(blob_to_string(((Blob) rs.getObject(1))), test_str1);
        assertEquals(blob_to_string(((Blob) rs.getObject("a"))), test_str1);
        assertEquals(blob_to_string(((Blob) rs.getObject("c"))), test_str2);
        assertNull(rs.getObject(2));
        assertNull(rs.getObject("b"));

        rs.close();
        stmt.close();
        conn.close();
    }

    public static void test_uuid() throws Exception {
        // Generated by DuckDB
        String testUuid = "a0a34a0a-1794-47b6-b45c-0ac68cc03702";

        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement();
             DuckDBResultSet rs = stmt.executeQuery("SELECT a, NULL::UUID b, a::VARCHAR c, '" + testUuid +
                                                    "'::UUID d FROM (SELECT uuid() a)")
                                      .unwrap(DuckDBResultSet.class)) {
            assertTrue(rs.next());

            // UUID direct
            UUID a = (UUID) rs.getObject(1);
            assertTrue(a != null);
            assertTrue(rs.getObject("a") instanceof UUID);
            assertFalse(rs.wasNull());

            // Null handling
            assertNull(rs.getObject(2));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("b"));
            assertTrue(rs.wasNull());

            // String interpreted as UUID in Java, rather than in DuckDB
            assertTrue(rs.getObject(3) instanceof String);
            assertEquals(rs.getUuid(3), a);
            assertFalse(rs.wasNull());

            // Verify UUID computation is correct
            assertEquals(rs.getObject(4), UUID.fromString(testUuid));
        }
    }

    public static void test_unsigned_integers() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
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

    public static void test_get_schema() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);

        assertEquals(conn.getSchema(), DuckDBConnection.DEFAULT_SCHEMA);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA alternate_schema;");
            stmt.execute("SET search_path = \"alternate_schema\";");
        }

        assertEquals(conn.getSchema(), "alternate_schema");

        conn.setSchema("main");
        assertEquals(conn.getSchema(), "main");

        conn.close();

        try {
            conn.getSchema();
            fail();
        } catch (SQLException e) {
            assertEquals(e.getMessage(), "Connection Error: Invalid connection");
        }
    }

    /**
     * @see {https://github.com/duckdb/duckdb/issues/3906}
     */
    public static void test_cached_row_set() throws Exception {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        rowSet.setUrl("jdbc:duckdb:");
        rowSet.setCommand("select 1");
        rowSet.execute();

        rowSet.next();
        assertEquals(rowSet.getInt(1), 1);
    }

    public static void test_json() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select [1, 5]::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.JAVA_OBJECT);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isArray());
            assertEquals(jsonNode.toString(), "[1,5]");
        }

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select '{\"key\": \"value\"}'::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.JAVA_OBJECT);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isObject());
            assertEquals(jsonNode.toString(),
                         "{\"key\": \"value\"}"); // this isn't valid json output, must load json extension for that
        }

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select '\"hello\"'::JSON");
            rs.next();
            assertEquals(rs.getMetaData().getColumnType(1), Types.JAVA_OBJECT);
            JsonNode jsonNode = (JsonNode) rs.getObject(1);
            assertTrue(jsonNode.isString());
            assertEquals(jsonNode.toString(), "\"hello\"");
        }
    }

    public static void test_bug4218_prepare_types() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
        String query = "SELECT ($1 || $2)";
        conn.prepareStatement(query);
        assertTrue(true);
    }

    public static void test_bug532_timestamp() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();

        ResultSet rs;

        stmt.execute("CREATE TABLE t0(c0 DATETIME);");
        stmt.execute("INSERT INTO t0 VALUES(DATE '1-1-1');");
        rs = stmt.executeQuery("SELECT t0.c0 FROM t0; ");

        rs.next();
        rs.getObject(1);
    }

    public static void test_bug966_typeof() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select typeof(1);");

        rs.next();
        assertEquals(rs.getString(1), "INTEGER");
    }

    public static void test_config() throws Exception {
        String memory_limit = "memory_limit";
        String threads = "threads";

        Properties info = new Properties();
        info.put(memory_limit, "500MB");
        info.put(threads, "5");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", info);

        assertEquals("500.0MB", getSetting(conn, memory_limit));
        assertEquals("5", getSetting(conn, threads));
    }

    public static void test_invalid_config() throws Exception {
        Properties info = new Properties();
        info.put("invalid config name", "true");

        String message = assertThrows(() -> DriverManager.getConnection("jdbc:duckdb:", info), SQLException.class);

        assertTrue(message.contains("Unrecognized configuration property \"invalid config name\""));
    }

    public static void test_valid_but_local_config_throws_exception() throws Exception {
        Properties info = new Properties();
        info.put("ordered_aggregate_threshold", "123");

        String message = assertThrows(() -> DriverManager.getConnection("jdbc:duckdb:", info), SQLException.class);

        assertTrue(message.contains("Failed to set configuration option \"ordered_aggregate_threshold\""));
    }

    private static String getSetting(Connection conn, String settingName) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement("select value from duckdb_settings() where name = ?")) {
            stmt.setString(1, settingName);
            ResultSet rs = stmt.executeQuery();
            rs.next();

            return rs.getString(1);
        }
    }

    public static void test_describe() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TEST (COL INT DEFAULT 42)");

            ResultSet rs = stmt.executeQuery("DESCRIBE SELECT * FROM TEST");
            rs.next();
            assertEquals(rs.getString("column_name"), "COL");
            assertEquals(rs.getString("column_type"), "INTEGER");
            assertEquals(rs.getString("null"), "YES");
            assertNull(rs.getString("key"));
            assertNull(rs.getString("default"));
            assertNull(rs.getString("extra"));
        }
    }

    public static void test_null_bytes_in_string() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (PreparedStatement stmt = conn.prepareStatement("select ?::varchar")) {
                stmt.setObject(1, "bob\u0000r");
                ResultSet rs = stmt.executeQuery();

                rs.next();
                assertEquals(rs.getString(1), "bob\u0000r");
            }
        }
    }

    public static void test_get_functions() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            ResultSet functions =
                conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "string_split");

            assertTrue(functions.next());
            assertNull(functions.getObject("FUNCTION_CAT"));
            assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
            assertEquals("string_split", functions.getString("FUNCTION_NAME"));
            assertEquals(DatabaseMetaData.functionNoTable, functions.getInt("FUNCTION_TYPE"));

            assertFalse(functions.next());

            // two items for two overloads?
            functions = conn.getMetaData().getFunctions(null, DuckDBConnection.DEFAULT_SCHEMA, "read_csv_auto");
            assertTrue(functions.next());
            assertNull(functions.getObject("FUNCTION_CAT"));
            assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
            assertEquals("read_csv_auto", functions.getString("FUNCTION_NAME"));
            assertEquals(DatabaseMetaData.functionReturnsTable, functions.getInt("FUNCTION_TYPE"));

            assertTrue(functions.next());
            assertNull(functions.getObject("FUNCTION_CAT"));
            assertEquals(DuckDBConnection.DEFAULT_SCHEMA, functions.getString("FUNCTION_SCHEM"));
            assertEquals("read_csv_auto", functions.getString("FUNCTION_NAME"));
            assertEquals(DatabaseMetaData.functionReturnsTable, functions.getInt("FUNCTION_TYPE"));

            assertFalse(functions.next());
        }
    }

    public static void test_get_primary_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();) {
            Object[][] testData = new Object[12][6];
            int testDataIndex = 0;

            Object[][] params = new Object[6][5];
            int paramIndex = 0;

            String catalog = conn.getCatalog();

            for (int schemaNumber = 1; schemaNumber <= 2; schemaNumber++) {
                String schemaName = "schema" + schemaNumber;
                stmt.executeUpdate("CREATE SCHEMA " + schemaName);
                stmt.executeUpdate("SET SCHEMA = '" + schemaName + "'");
                for (int tableNumber = 1; tableNumber <= 3; tableNumber++) {
                    String tableName = "table" + tableNumber;
                    params[paramIndex] = new Object[] {catalog, schemaName, tableName, testDataIndex, -1};
                    String columns = null;
                    String pk = null;
                    for (int columnNumber = 1; columnNumber <= tableNumber; columnNumber++) {
                        String columnName = "column" + columnNumber;
                        String columnDef = columnName + " int not null";
                        columns = columns == null ? columnDef : columns + "," + columnDef;
                        pk = pk == null ? columnName : pk + "," + columnName;
                        testData[testDataIndex++] =
                            new Object[] {catalog, schemaName, tableName, columnName, columnNumber, null};
                    }
                    stmt.executeUpdate("CREATE TABLE " + tableName + "(" + columns + ",PRIMARY KEY(" + pk + ") )");
                    params[paramIndex][4] = testDataIndex;
                    paramIndex += 1;
                }
            }

            DatabaseMetaData databaseMetaData = conn.getMetaData();
            for (paramIndex = 0; paramIndex < 6; paramIndex++) {
                Object[] paramSet = params[paramIndex];
                ResultSet resultSet =
                    databaseMetaData.getPrimaryKeys((String) paramSet[0], (String) paramSet[1], (String) paramSet[2]);
                for (testDataIndex = (int) paramSet[3]; testDataIndex < (int) paramSet[4]; testDataIndex++) {
                    assertTrue(resultSet.next(), "Expected a row at position " + testDataIndex);
                    Object[] testDataRow = testData[testDataIndex];
                    for (int columnIndex = 0; columnIndex < testDataRow.length; columnIndex++) {
                        Object value = testDataRow[columnIndex];
                        if (value == null || value instanceof String) {
                            String columnValue = resultSet.getString(columnIndex + 1);
                            assertTrue(value == null ? columnValue == null : value.equals(columnValue),
                                       "row value " + testDataIndex + ", " + columnIndex + " " + value +
                                           " should equal column value " + columnValue);
                        } else {
                            int testValue = ((Integer) value).intValue();
                            int columnValue = resultSet.getInt(columnIndex + 1);
                            assertTrue(testValue == columnValue, "row value " + testDataIndex + ", " + columnIndex +
                                                                     " " + testValue + " should equal column value " +
                                                                     columnValue);
                        }
                    }
                }
                resultSet.close();
            }

            /*
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println("WITH constraint_columns as (");
            pw.println("select");
            pw.println("  database_name as \"TABLE_CAT\"");
            pw.println(", schema_name as \"TABLE_SCHEM\"");
            pw.println(", table_name as \"TABLE_NAME\"");
            pw.println(", unnest(constraint_column_names) as \"COLUMN_NAME\"");
            pw.println(", cast(null as varchar) as \"PK_NAME\"");
            pw.println("from duckdb_constraints");
            pw.println("where constraint_type = 'PRIMARY KEY'");
            pw.println(")");
            pw.println("SELECT \"TABLE_CAT\"");
            pw.println(", \"TABLE_SCHEM\"");
            pw.println(", \"TABLE_NAME\"");
            pw.println(", \"COLUMN_NAME\"");
            pw.println(", cast(row_number() over ");
            pw.println("(partition by \"TABLE_CAT\", \"TABLE_SCHEM\", \"TABLE_NAME\") as int) as \"KEY_SEQ\"");
            pw.println(", \"PK_NAME\"");
            pw.println("FROM constraint_columns");
            pw.println("ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, KEY_SEQ");

            ResultSet resultSet = stmt.executeQuery(sw.toString());
            ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, catalog);
            for (testDataIndex = 0; testDataIndex < testData.length; testDataIndex++) {
                assertTrue(resultSet.next(), "Expected a row at position " + testDataIndex);
                Object[] testDataRow = testData[testDataIndex];
                for (int columnIndex = 0; columnIndex < testDataRow.length; columnIndex++) {
                    Object value = testDataRow[columnIndex];
                    if (value == null || value instanceof String) {
                        String columnValue = resultSet.getString(columnIndex + 1);
                        assertTrue(
                            value == null ? columnValue == null : value.equals(columnValue),
                            "row value " + testDataIndex + ", " + columnIndex + " " + value +
                            " should equal column value "+ columnValue
                        );
                    } else {
                        int testValue = ((Integer) value).intValue();
                        int columnValue = resultSet.getInt(columnIndex + 1);
                        assertTrue(
                            testValue == columnValue,
                            "row value " + testDataIndex + ", " + columnIndex + " " + testValue +
                            " should equal column value " + columnValue
                        );
                    }
                }
            }
            */
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void test_instance_cache() throws Exception {
        Path database_file = Files.createTempFile("duckdb-instance-cache-test-", ".duckdb");
        database_file.toFile().delete();

        String jdbc_url = "jdbc:duckdb:" + database_file.toString();

        Connection conn = DriverManager.getConnection(jdbc_url);
        Connection conn2 = DriverManager.getConnection(jdbc_url);

        conn.close();
        conn2.close();
    }

    public static void test_user_password() throws Exception {
        String jdbc_url = "jdbc:duckdb:";
        Properties p = new Properties();
        p.setProperty("user", "wilbur");
        p.setProperty("password", "quack");
        Connection conn = DriverManager.getConnection(jdbc_url, p);
        conn.close();

        Properties p2 = new Properties();
        p2.setProperty("User", "wilbur");
        p2.setProperty("PASSWORD", "quack");
        Connection conn2 = DriverManager.getConnection(jdbc_url, p2);
        conn2.close();
    }

    public static void test_readonly_remains_bug5593() throws Exception {
        Path database_file = Files.createTempFile("duckdb-instance-cache-test-", ".duckdb");
        database_file.toFile().delete();
        String jdbc_url = "jdbc:duckdb:" + database_file.toString();

        Properties p = new Properties();
        p.setProperty("duckdb.read_only", "true");
        try {
            Connection conn = DriverManager.getConnection(jdbc_url, p);
            conn.close();
        } catch (Exception e) {
            // nop
        }
        assertTrue(p.containsKey("duckdb.read_only"));
    }

    public static void test_supportsLikeEscapeClause_shouldBe_true() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        assertTrue(databaseMetaData.supportsLikeEscapeClause(),
                   "DatabaseMetaData.supportsLikeEscapeClause() should be true.");
    }

    public static void test_supports_catalogs_in_table_definitions() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String IS_TablesQuery = "SELECT * FROM information_schema.tables " +
                                      String.format("WHERE table_catalog = '%s' ", CATALOG_NAME) +
                                      String.format("AND table_name = '%s'", TABLE_NAME);
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;
        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));

            final boolean supportsCatalogsInTableDefinitions = databaseMetaData.supportsCatalogsInTableDefinitions();
            try {
                statement.execute(String.format("CREATE TABLE %s (id int)", QUALIFIED_TABLE_NAME));
            } catch (SQLException ex) {
                if (supportsCatalogsInTableDefinitions) {
                    fail(
                        "supportsCatalogsInTableDefinitions is true but CREATE TABLE in attached database is not allowed. " +
                        ex.getMessage());
                    ex.printStackTrace();
                }
            }
            resultSet = statement.executeQuery(IS_TablesQuery);
            assertTrue(resultSet.next(), "Expected exactly 1 row from information_schema.tables, got 0");
            assertFalse(resultSet.next());
            resultSet.close();

            try {
                statement.execute(String.format("DROP TABLE %s", QUALIFIED_TABLE_NAME));
            } catch (SQLException ex) {
                if (supportsCatalogsInTableDefinitions) {
                    fail(
                        "supportsCatalogsInTableDefinitions is true but DROP TABLE in attached database is not allowed. " +
                        ex.getMessage());
                    ex.printStackTrace();
                }
            }
            resultSet = statement.executeQuery(IS_TablesQuery);
            assertTrue(resultSet.next() == false, "Expected exactly 0 rows from information_schema.tables, got > 0");
            resultSet.close();

            assertTrue(supportsCatalogsInTableDefinitions, "supportsCatalogsInTableDefinitions should return true.");
        }
    }

    public static void test_supports_catalogs_in_data_manipulation() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String COLUMN_NAME = "id";
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;

        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));
            statement.execute(String.format("CREATE TABLE %s(%s int)", QUALIFIED_TABLE_NAME, COLUMN_NAME));

            final boolean supportsCatalogsInDataManipulation = databaseMetaData.supportsCatalogsInDataManipulation();
            try {
                statement.execute(String.format("INSERT INTO %s VALUES(1)", QUALIFIED_TABLE_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from " + QUALIFIED_TABLE_NAME + ", got 0");
                assertTrue(resultSet.getInt(COLUMN_NAME) == 1, "Value for " + COLUMN_NAME + " should be 1");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but INSERT in " + QUALIFIED_TABLE_NAME +
                         " is not allowed." + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute(
                    String.format("UPDATE %1$s SET %2$s = 2 WHERE %2$s = 1", QUALIFIED_TABLE_NAME, COLUMN_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from " + QUALIFIED_TABLE_NAME + ", got 0");
                assertTrue(resultSet.getInt(COLUMN_NAME) == 2, "Value for " + COLUMN_NAME + " should be 2");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but UPDATE of " + QUALIFIED_TABLE_NAME +
                         " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute(String.format("DELETE FROM %s WHERE %s = 2", QUALIFIED_TABLE_NAME, COLUMN_NAME));
                resultSet = statement.executeQuery(String.format("SELECT * FROM %s", QUALIFIED_TABLE_NAME));
                assertTrue(resultSet.next() == false, "Expected 0 rows from " + QUALIFIED_TABLE_NAME + ", got > 0");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInDataManipulation) {
                    fail("supportsCatalogsInDataManipulation is true but UPDATE of " + QUALIFIED_TABLE_NAME +
                         " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            assertTrue(supportsCatalogsInDataManipulation, "supportsCatalogsInDataManipulation should return true.");
        }
    }

    public static void test_supports_catalogs_in_index_definitions() throws Exception {
        final String CATALOG_NAME = "tmp";
        final String TABLE_NAME = "t1";
        final String INDEX_NAME = "idx1";
        final String QUALIFIED_TABLE_NAME = CATALOG_NAME + "." + TABLE_NAME;
        final String QUALIFIED_INDEX_NAME = CATALOG_NAME + "." + INDEX_NAME;

        ResultSet resultSet = null;
        try (final Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             final Statement statement = connection.createStatement();) {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            statement.execute(String.format("ATTACH '' AS \"%s\"", CATALOG_NAME));

            final boolean supportsCatalogsInIndexDefinitions = databaseMetaData.supportsCatalogsInIndexDefinitions();
            try {
                statement.execute(String.format("CREATE TABLE %s(id int)", QUALIFIED_TABLE_NAME));
                statement.execute(String.format("CREATE INDEX %s ON %s(id)", INDEX_NAME, QUALIFIED_TABLE_NAME));
                resultSet = statement.executeQuery(
                    String.format("SELECT * FROM duckdb_indexes() "
                                      + "WHERE database_name = '%s' AND table_name = '%s' AND index_name = '%s' ",
                                  CATALOG_NAME, TABLE_NAME, INDEX_NAME));
                assertTrue(resultSet.next(), "Expected exactly 1 row from duckdb_indexes(), got 0");
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInIndexDefinitions) {
                    fail("supportsCatalogsInIndexDefinitions is true but "
                         + "CREATE INDEX on " + QUALIFIED_TABLE_NAME + " is not allowed. " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            try {
                statement.execute("DROP index " + QUALIFIED_INDEX_NAME);
                resultSet = statement.executeQuery(
                    String.format("SELECT * FROM duckdb_indexes() "
                                      + "WHERE database_name = '%s' AND table_name = '%s' AND index_name = '%s'",
                                  CATALOG_NAME, TABLE_NAME, INDEX_NAME));
                assertFalse(resultSet.next());
                resultSet.close();
            } catch (SQLException ex) {
                if (supportsCatalogsInIndexDefinitions) {
                    fail("supportsCatalogsInIndexDefinitions is true but DROP of " + QUALIFIED_INDEX_NAME +
                         " is not allowed." + ex.getMessage());
                    ex.printStackTrace();
                }
            }

            assertTrue(supportsCatalogsInIndexDefinitions, "supportsCatalogsInIndexDefinitions should return true.");
        }
    }

    public static void test_structs() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement statement = connection.prepareStatement("select {\"a\": 1}")) {
            ResultSet resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            Struct struct = (Struct) resultSet.getObject(1);
            assertEquals(toJavaObject(struct), mapOf("a", 1));
            assertEquals(struct.getSQLTypeName(), "STRUCT(a INTEGER)");
        }
    }

    public static void test_union() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE tbl1(u UNION(num INT, str VARCHAR));");
            statement.execute("INSERT INTO tbl1 values (1) , ('two') , (union_value(str := 'three'));");

            ResultSet rs = statement.executeQuery("select * from tbl1");
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), 1);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), "two");
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), "three");
        }
    }

    public static void test_list() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("select [1]")) {
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), singletonList(1));
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[1], [42, 69]])")) {
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), singletonList(1));
                assertTrue(rs.next());
                assertEquals(arrayToList(rs.getArray(1)), asList(42, 69));
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[[42], [69]]])")) {
                assertTrue(rs.next());

                List<List<Integer>> expected = asList(singletonList(42), singletonList(69));
                List<Array> actual = arrayToList(rs.getArray(1));

                for (int i = 0; i < actual.size(); i++) {
                    assertEquals(actual.get(i), expected.get(i));
                }
            }
            try (ResultSet rs = statement.executeQuery("select unnest([[], [69]])")) {
                assertTrue(rs.next());
                assertTrue(arrayToList(rs.getArray(1)).isEmpty());
            }
        }
    }

    private static <T> List<T> arrayToList(Array array) throws SQLException {
        return arrayToList((T[]) array.getArray());
    }

    private static <T> List<T> arrayToList(T[] array) throws SQLException {
        List<T> out = new ArrayList<>();
        for (Object t : array) {
            out.add((T) toJavaObject(t));
        }
        return out;
    }

    private static Object toJavaObject(Object t) {
        try {
            if (t instanceof Array) {
                t = arrayToList((Array) t);
            } else if (t instanceof Struct) {
                t = structToMap((DuckDBStruct) t);
            }
            return t;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void test_map() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement statement = connection.prepareStatement("select map([100, 5], ['a', 'b'])")) {
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), mapOf(100, "a", 5, "b"));
        }
    }

    public static void test_extension_type() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
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
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();) {
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

    public static void test_getColumnClassName() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement s = conn.createStatement();) {
            try (ResultSet rs = s.executeQuery("select * from test_all_types()")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                rs.next();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    Object value = rs.getObject(i);

                    assertEquals(rsmd.getColumnClassName(i), value.getClass().getName());
                }
            }
        }
    }

    public static void test_update_count() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement s = connection.createStatement()) {
            s.execute("create table t (i int)");
            assertEquals(s.getUpdateCount(), -1);
            assertEquals(s.executeUpdate("insert into t values (1)"), 1);
            assertFalse(s.execute("insert into t values (1)"));
            assertEquals(s.getUpdateCount(), 1);

            // result is invalidated after a call
            assertEquals(s.getUpdateCount(), -1);
        }
    }

    public static void test_get_result_set() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (PreparedStatement p = conn.prepareStatement("select 1")) {
                p.executeQuery();
                try (ResultSet resultSet = p.getResultSet()) {
                    assertNotNull(resultSet);
                }
                assertNull(p.getResultSet()); // returns null after initial call
            }

            try (Statement s = conn.createStatement()) {
                s.execute("select 1");
                try (ResultSet resultSet = s.getResultSet()) {
                    assertNotNull(resultSet);
                }
                assertFalse(s.getMoreResults());
                assertNull(s.getResultSet()); // returns null after initial call
            }
        }
    }

    // https://github.com/duckdb/duckdb/issues/7218
    public static void test_unknown_result_type() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement p = connection.prepareStatement(
                 "select generate_series.generate_series from generate_series(?, ?) order by 1")) {
            p.setInt(1, 0);
            p.setInt(2, 1);

            try (ResultSet rs = p.executeQuery()) {
                rs.next();
                assertEquals(rs.getInt(1), 0);
                rs.next();
                assertEquals(rs.getInt(1), 1);
            }
        }
    }

    static List<Object> trio(Object... max) {
        return asList(emptyList(), asList(max), null);
    }

    static DuckDBResultSet.DuckDBBlobResult blobOf(String source) {
        return new DuckDBResultSet.DuckDBBlobResult(ByteBuffer.wrap(source.getBytes()));
    }

    private static final DateTimeFormatter FORMAT_DATE = new DateTimeFormatterBuilder()
                                                             .parseCaseInsensitive()
                                                             .appendValue(YEAR_OF_ERA)
                                                             .appendLiteral('-')
                                                             .appendValue(MONTH_OF_YEAR, 2)
                                                             .appendLiteral('-')
                                                             .appendValue(DAY_OF_MONTH, 2)
                                                             .toFormatter()
                                                             .withResolverStyle(ResolverStyle.LENIENT);
    public static final DateTimeFormatter FORMAT_DATETIME = new DateTimeFormatterBuilder()
                                                                .append(FORMAT_DATE)
                                                                .appendLiteral('T')
                                                                .append(ISO_LOCAL_TIME)
                                                                .toFormatter()
                                                                .withResolverStyle(ResolverStyle.LENIENT);
    public static final DateTimeFormatter FORMAT_TZ = new DateTimeFormatterBuilder()
                                                          .append(FORMAT_DATETIME)
                                                          .appendLiteral('+')
                                                          .appendValue(OFFSET_SECONDS)
                                                          .toFormatter()
                                                          .withResolverStyle(ResolverStyle.LENIENT);

    static <K, V> Map<K, V> mapOf(Object... pairs) {
        Map<K, V> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length - 1; i += 2) {
            result.put((K) pairs[i], (V) pairs[i + 1]);
        }
        return result;
    }

    static Map<String, List<Object>> correct_answer_map = new HashMap<>();
    static {
        correct_answer_map.put("int_array", trio(42, 999, null, null, -42));
        correct_answer_map.put("double_array",
                               trio(42.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, null, -42.0));
        correct_answer_map.put(
            "date_array", trio(LocalDate.parse("1970-01-01"), LocalDate.parse("999999999-12-31", FORMAT_DATE),
                               LocalDate.parse("-999999999-01-01", FORMAT_DATE), null, LocalDate.parse("2022-05-12")));
        correct_answer_map.put("timestamp_array", trio(Timestamp.valueOf("1970-01-01 00:00:00.0"),
                                                       DuckDBTimestamp.toSqlTimestamp(9223372036854775807L),
                                                       DuckDBTimestamp.toSqlTimestamp(-9223372036854775807L), null,
                                                       Timestamp.valueOf("2022-05-12 16:23:45.0")));
        correct_answer_map.put("timestamptz_array", trio(OffsetDateTime.parse("1970-01-01T00:00Z"),
                                                         OffsetDateTime.parse("+294247-01-10T04:00:54.775807Z"),
                                                         OffsetDateTime.parse("-290308-12-21T19:59:05.224193Z"), null,
                                                         OffsetDateTime.parse("2022-05-12T23:23:45Z")));
        correct_answer_map.put("varchar_array", trio("🦆🦆🦆🦆🦆🦆", "goose", null, ""));
        correct_answer_map.put("nested_int_array", trio(emptyList(), asList(42, 999, null, null, -42), null,
                                                        emptyList(), asList(42, 999, null, null, -42)));
        correct_answer_map.put("struct_of_arrays", asList(mapOf("a", null, "b", null),
                                                          mapOf("a", asList(42, 999, null, null, -42), "b",
                                                                asList("🦆🦆🦆🦆🦆🦆", "goose", null, "")),
                                                          null));
        correct_answer_map.put("array_of_structs", trio(mapOf("a", null, "b", null),
                                                        mapOf("a", 42, "b", "🦆🦆🦆🦆🦆🦆"), null));
        correct_answer_map.put("bool", asList(false, true, null));
        correct_answer_map.put("tinyint", asList((byte) -128, (byte) 127, null));
        correct_answer_map.put("smallint", asList((short) -32768, (short) 32767, null));
        correct_answer_map.put("int", asList(-2147483648, 2147483647, null));
        correct_answer_map.put("bigint", asList(-9223372036854775808L, 9223372036854775807L, null));
        correct_answer_map.put("hugeint", asList(new BigInteger("-170141183460469231731687303715884105727"),
                                                 new BigInteger("170141183460469231731687303715884105727"), null));
        correct_answer_map.put("utinyint", asList((short) 0, (short) 255, null));
        correct_answer_map.put("usmallint", asList(0, 65535, null));
        correct_answer_map.put("uint", asList(0L, 4294967295L, null));
        correct_answer_map.put("ubigint", asList(BigInteger.ZERO, new BigInteger("18446744073709551615"), null));
        correct_answer_map.put("time", asList(LocalTime.of(0, 0), LocalTime.parse("23:59:59.999999"), null));
        correct_answer_map.put("float", asList(-3.4028234663852886e+38f, 3.4028234663852886e+38f, null));
        correct_answer_map.put("double", asList(-1.7976931348623157e+308d, 1.7976931348623157e+308d, null));
        correct_answer_map.put("dec_4_1", asList(new BigDecimal("-999.9"), (new BigDecimal("999.9")), null));
        correct_answer_map.put("dec_9_4", asList(new BigDecimal("-99999.9999"), (new BigDecimal("99999.9999")), null));
        correct_answer_map.put(
            "dec_18_6", asList(new BigDecimal("-999999999999.999999"), (new BigDecimal("999999999999.999999")), null));
        correct_answer_map.put("dec38_10", asList(new BigDecimal("-9999999999999999999999999999.9999999999"),
                                                  (new BigDecimal("9999999999999999999999999999.9999999999")), null));
        correct_answer_map.put("uuid", asList(UUID.fromString("00000000-0000-0000-0000-000000000001"),
                                              (UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")), null));
        correct_answer_map.put("varchar", asList("🦆🦆🦆🦆🦆🦆", "goo\u0000se", null));
        correct_answer_map.put("json", asList("🦆🦆🦆🦆🦆", "goose", null));
        correct_answer_map.put(
            "blob", asList(blobOf("thisisalongblob\u0000withnullbytes"), blobOf("\u0000\u0000\u0000a"), null));
        correct_answer_map.put("bit", asList("0010001001011100010101011010111", "10101", null));
        correct_answer_map.put("small_enum", asList("DUCK_DUCK_ENUM", "GOOSE", null));
        correct_answer_map.put("medium_enum", asList("enum_0", "enum_299", null));
        correct_answer_map.put("large_enum", asList("enum_0", "enum_69999", null));
        correct_answer_map.put(
            "struct", asList(mapOf("a", null, "b", null), mapOf("a", 42, "b", "🦆🦆🦆🦆🦆🦆"), null));
        correct_answer_map.put("map",
                               asList(mapOf(), mapOf("key1", "🦆🦆🦆🦆🦆🦆", "key2", "goose"), null));
        correct_answer_map.put("union", asList("Frank", (short) 5, null));
        correct_answer_map.put(
            "time_tz", asList(OffsetTime.parse("00:00+00:00"), OffsetTime.parse("23:59:59.999999+00:00"), null));
        correct_answer_map.put("interval", asList("00:00:00", "83 years 3 months 999 days 00:16:39.999999", null));
        correct_answer_map.put("timestamp", asList(DuckDBTimestamp.toSqlTimestamp(-9223372022400000000L),
                                                   DuckDBTimestamp.toSqlTimestamp(9223372036854775807L), null));
        correct_answer_map.put("date", asList(LocalDate.of(-5877641, 6, 25), LocalDate.of(5881580, 7, 10), null));
        correct_answer_map.put("timestamp_s",
                               asList(Timestamp.valueOf(LocalDateTime.of(-290308, 12, 22, 0, 0)),
                                      Timestamp.valueOf(LocalDateTime.of(294247, 1, 10, 4, 0, 54)), null));
        correct_answer_map.put("timestamp_ns",
                               asList(Timestamp.valueOf(LocalDateTime.parse("1677-09-21T00:12:43.145225")),
                                      Timestamp.valueOf(LocalDateTime.parse("2262-04-11T23:47:16.854775")), null));
        correct_answer_map.put("timestamp_ms",
                               asList(Timestamp.valueOf(LocalDateTime.of(-290308, 12, 22, 0, 0, 0)),
                                      Timestamp.valueOf(LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775000000)), null));
        correct_answer_map.put(
            "timestamp_tz",
            asList(OffsetDateTime.of(LocalDateTime.of(-290308, 12, 22, 0, 0, 0), ZoneOffset.UTC),
                   OffsetDateTime.of(LocalDateTime.of(294247, 1, 10, 4, 0, 54, 776806000), ZoneOffset.UTC), null));
    }

    public static void test_all_types() throws Exception {
        Logger logger = Logger.getAnonymousLogger();

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement stmt = conn.prepareStatement("select * from test_all_types()")) {
            conn.createStatement().execute("set timezone = 'UTC'");

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();

                int rowIdx = 0;
                while (rs.next()) {
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        List<Object> answers = correct_answer_map.get(columnName);
                        Object expected = answers.get(rowIdx);

                        Object actual = toJavaObject(rs.getObject(i + 1));

                        if (actual instanceof Timestamp && expected instanceof Timestamp) {
                            assertEquals(((Timestamp) actual).getTime(), ((Timestamp) expected).getTime(), 500);
                        } else if (actual instanceof OffsetDateTime && expected instanceof OffsetDateTime) {
                            assertEquals(((OffsetDateTime) actual).getLong(MILLI_OF_SECOND),
                                         ((OffsetDateTime) expected).getLong(MILLI_OF_SECOND), 5000);
                        } else if (actual instanceof List) {
                            assertListsEqual((List) actual, (List) expected);
                        } else {
                            assertEquals(actual, expected);
                        }
                    }
                    rowIdx++;
                }
            }
        }
    }

    private static Map<String, Object> structToMap(DuckDBStruct actual) throws SQLException {
        Map<String, Object> map = actual.getMap();
        Map<String, Object> result = new HashMap<>();
        map.forEach((key, value) -> result.put(key, toJavaObject(value)));
        return result;
    }

    private static <T> void assertListsEqual(List<T> actual, List<T> expected) throws Exception {
        assertEquals(actual.size(), expected.size());

        ListIterator<T> itera = actual.listIterator();
        ListIterator<T> itere = expected.listIterator();

        while (itera.hasNext()) {
            assertEquals(itera.next(), itere.next());
        }
    }

    public static void test_cancel() throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(1);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement()) {
            Future<String> thread = service.submit(
                ()
                    -> assertThrows(()
                                        -> stmt.execute("select count(*) from range(10000000) t1, range(1000000) t2;"),
                                    SQLException.class));
            Thread.sleep(500); // wait for query to start running
            stmt.cancel();
            String message = thread.get(1, TimeUnit.SECONDS);
            assertEquals(message, "INTERRUPT Error: Interrupted!");
        }
    }

    public static void test_prepared_statement_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement stmt = conn.prepareStatement("SELECT 'hello' as world")) {
            ResultSetMetaData metadata = stmt.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);
            assertEquals(metadata.getColumnName(1), "world");
            assertEquals(metadata.getColumnType(1), Types.VARCHAR);
        }
    }

    public static void test_unbindable_query() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement stmt = conn.prepareStatement("SELECT ?, ?")) {
            stmt.setString(1, "word1");
            stmt.setInt(2, 42);

            ResultSetMetaData meta = stmt.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "unknown");
            assertEquals(meta.getColumnTypeName(1), "UNKNOWN");
            assertEquals(meta.getColumnType(1), Types.JAVA_OBJECT);

            try (ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData metadata = resultSet.getMetaData();

                assertEquals(metadata.getColumnCount(), 2);

                assertEquals(metadata.getColumnName(1), "$1");
                assertEquals(metadata.getColumnTypeName(1), "VARCHAR");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);

                assertEquals(metadata.getColumnName(2), "$2");
                assertEquals(metadata.getColumnTypeName(2), "INTEGER");
                assertEquals(metadata.getColumnType(2), Types.INTEGER);

                resultSet.next();
                assertEquals(resultSet.getString(1), "word1");
                assertEquals(resultSet.getInt(2), 42);
            }
        }
    }

    public static void test_labels_with_prepped_statement() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as result")) {
                stmt.setString(1, "Quack");
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        assertEquals(rs.getObject("result"), "Quack");
                    }
                }
            }
        }
    }

    public static void test_execute_updated_on_prep_stmt() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement s = conn.createStatement()) {
            s.executeUpdate("create table t (i int)");

            try (PreparedStatement p = conn.prepareStatement("insert into t (i) select ?")) {
                p.setInt(1, 1);
                p.executeUpdate();
            }
        }
    }

    public static void test_invalid_execute_calls() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement s = conn.createStatement()) {
                s.execute("create table test (id int)");
            }
            try (PreparedStatement s = conn.prepareStatement("select 1")) {
                String msg = assertThrows(s::executeUpdate, SQLException.class);
                assertTrue(msg.contains("can only be used with queries that return nothing") &&
                           msg.contains("or update rows"));
            }
            try (PreparedStatement s = conn.prepareStatement("insert into test values (1)")) {
                String msg = assertThrows(s::executeQuery, SQLException.class);
                assertTrue(msg.contains("can only be used with queries that return a ResultSet"));
            }
        }
    }

    public static void test_race() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            ExecutorService executorService = Executors.newFixedThreadPool(10);

            List<Callable<Object>> tasks = Collections.nCopies(1000, () -> {
                try {
                    try (PreparedStatement ps = connection.prepareStatement(
                             "SELECT count(*) FROM information_schema.tables WHERE table_name = 'test' LIMIT 1;")) {
                        ps.execute();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            List<Future<Object>> results = executorService.invokeAll(tasks);

            try {
                for (Future<Object> future : results) {
                    future.get();
                }
                fail("Should have thrown an exception");
            } catch (java.util.concurrent.ExecutionException ee) {
                assertEquals(
                    ee.getCause().getCause().getMessage(),
                    "Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Woo I can do reflection too, take this, JUnit!
        Method[] methods = TestDuckDBJDBC.class.getMethods();

        Arrays.sort(methods, new Comparator<Method>() {
            @Override
            public int compare(Method o1, Method o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        String specific_test = null;
        if (args.length >= 1) {
            specific_test = args[0];
        }

        boolean anySucceeded = false;
        boolean anyFailed = false;
        for (Method m : methods) {
            if (m.getName().startsWith("test_")) {
                if (specific_test != null && !m.getName().contains(specific_test)) {
                    continue;
                }
                System.out.print(m.getName() + " ");

                LocalDateTime start = LocalDateTime.now();
                try {
                    m.invoke(null);
                    System.out.println("success in " + Duration.between(start, LocalDateTime.now()).getSeconds() +
                                       " seconds");
                } catch (Throwable t) {
                    if (t instanceof InvocationTargetException) {
                        t = t.getCause();
                    }
                    System.out.println("failed with " + t);
                    t.printStackTrace(System.out);
                    anyFailed = true;
                }
                anySucceeded = true;
            }
        }
        if (!anySucceeded) {
            System.out.println("No tests found that match " + specific_test);
            System.exit(1);
        }
        System.out.println(anyFailed ? "FAILED" : "OK");

        System.exit(anyFailed ? 1 : 0);
    }
}
