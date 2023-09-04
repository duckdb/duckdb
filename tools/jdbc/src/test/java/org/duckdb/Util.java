package org.duckdb;

import java.io.InputStream;
import java.io.IOException;

/**
 * this class exists only as bridge between the test and main packages
 */
public class Util {
    public  static byte[] readAllBytes(InputStream e) throws IOException {
        return JdbcUtils.readAllBytes(e);
    }
}

