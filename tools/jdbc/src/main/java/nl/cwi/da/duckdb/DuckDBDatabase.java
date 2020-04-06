package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;

public class DuckDBDatabase {


	// TODO how do we clean this up
	public DuckDBDatabase(String url) {
		db_ref = DuckDBNative.duckdb_jdbc_startup(url, false);

	}
	
	protected ByteBuffer db_ref;

}
