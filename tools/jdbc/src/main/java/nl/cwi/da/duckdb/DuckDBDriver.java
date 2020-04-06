package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DuckDBDriver implements java.sql.Driver {


	
	static {
		try {
			DriverManager.registerDriver(new DuckDBDriver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection connect(String url, Properties info) throws SQLException {
		DuckDBDatabase db = new DuckDBDatabase(url);
		return new DuckDBConnection(db);
	}

	public boolean acceptsURL(String url) throws SQLException {
		System.out.println(url);
		return url.startsWith("jdbc:duckdb");
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		System.out.println(url);

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
		return true; // of course
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("no logger");
	}

}
