package org.duckdb.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;

public class Runner {
    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static int runTests(String[] args, Class<?> testClass) {
        // Woo I can do reflection too, take this, JUnit!
        Method[] methods = testClass.getMethods();

        Arrays.sort(methods, Comparator.comparing(Method::getName));

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
            return 1;
        }
        System.out.println(anyFailed ? "FAILED" : "OK");

        return anyFailed ? 1 : 0;
    }
}
