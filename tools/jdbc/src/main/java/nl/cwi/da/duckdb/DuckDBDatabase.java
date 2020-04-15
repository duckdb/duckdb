package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class DuckDBDatabase {
	
	protected String url;

	public DuckDBDatabase(String url) throws SQLException {
		if (!url.startsWith("jdbc:duckdb")) {
			throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
		}
		this.url = url;
		String db_dir = url.replaceFirst("^jdbc:duckdb:", "").trim();
		if (db_dir.length() == 0) {
			db_dir = ":memory:";
		}
		db_ref = DuckDBNative.duckdb_jdbc_startup(db_dir.getBytes(StandardCharsets.UTF_8), false);
	}
	
	protected synchronized void finalize() throws Throwable {
		if (db_ref != null) {
			DuckDBNative.duckdb_jdbc_shutdown(db_ref);
			db_ref = null;
		}
	}
	
	protected ByteBuffer db_ref;

}
