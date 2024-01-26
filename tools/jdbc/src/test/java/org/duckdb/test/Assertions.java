package org.duckdb.test;

import org.duckdb.test.Thrower;

import java.util.Objects;
import java.util.function.Function;

public class Assertions {
    public static void assertTrue(boolean val) throws Exception {
        assertTrue(val, null);
    }

    public static void assertTrue(boolean val, String message) throws Exception {
        if (!val) {
            throw new Exception(message);
        }
    }

    public static void assertFalse(boolean val) throws Exception {
        assertTrue(!val);
    }

    public static void assertEquals(Object actual, Object expected) throws Exception {
        Function<Object, String> getClass = (Object a) -> a == null ? "null" : a.getClass().toString();

        String message = String.format("\"%s\" (of %s) should equal \"%s\" (of %s)", actual, getClass.apply(actual),
                                       expected, getClass.apply(expected));
        assertTrue(Objects.equals(actual, expected), message);
    }

    public static void assertNotNull(Object a) throws Exception {
        assertFalse(a == null);
    }

    public static void assertNull(Object a) throws Exception {
        assertEquals(a, null);
    }

    public static void assertEquals(double a, double b, double epsilon) throws Exception {
        assertTrue(Math.abs(a - b) < epsilon);
    }

    public static void fail() throws Exception {
        fail(null);
    }

    public static void fail(String s) throws Exception {
        throw new Exception(s);
    }

    public static <T extends Throwable> String assertThrows(Thrower thrower, Class<T> exception) throws Exception {
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

    // Asserts we are either throwing the correct exception, or not throwing at all
    public static <T extends Throwable> boolean assertThrowsMaybe(Thrower thrower, Class<T> exception)
        throws Exception {
        try {
            thrower.run();
            return true;
        } catch (Throwable e) {
            if (e.getClass().equals(exception)) {
                return true;
            } else {
                throw new Exception("Unexpected exception: " + e.getClass().getName());
            }
        }
    }
}
