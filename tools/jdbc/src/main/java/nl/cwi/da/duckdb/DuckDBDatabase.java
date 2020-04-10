package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public class DuckDBDatabase {

	public DuckDBDatabase(String url) throws SQLException {
		if (!url.startsWith("jdbc:duckdb")) {
			throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
		}
		String db_dir = url.replaceFirst("^jdbc:duckdb:", "").trim();
		if (db_dir.length() == 0) {
			db_dir = ":memory:";
		}
		db_ref = DuckDBNative.duckdb_jdbc_startup(db_dir, false);
	}
	
	protected synchronized void finalize() throws Throwable {
		if (db_ref != null) {
			DuckDBNative.duckdb_jdbc_shutdown(db_ref);
			db_ref = null;
		}
	}
	
	protected ByteBuffer db_ref;

}
