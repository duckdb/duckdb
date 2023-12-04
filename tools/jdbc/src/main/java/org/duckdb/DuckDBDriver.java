package org.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DuckDBDriver implements java.sql.Driver {

    public static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
    public static final String DUCKDB_USER_AGENT_PROPERTY = "custom_user_agent";
    public static final String JDBC_STREAM_RESULTS = "jdbc_stream_results";

    static {
        try {
            DriverManager.registerDriver(new DuckDBDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        boolean read_only = false;
        if (info == null) {
            info = new Properties();
        } else { // make a copy because we're removing the read only property below
            info = (Properties) info.clone();
        }
        String prop_val = (String) info.remove(DUCKDB_READONLY_PROPERTY);
        if (prop_val != null) {
            String prop_clean = prop_val.trim().toLowerCase();
            read_only = prop_clean.equals("1") || prop_clean.equals("true") || prop_clean.equals("yes");
        }
        info.put("duckdb_api", "jdbc");
        return DuckDBConnection.newConnection(url, read_only, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:duckdb:");
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo[] ret = {};
        return ret; // no properties
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return true; // of course!
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("no logger");
    }
}
