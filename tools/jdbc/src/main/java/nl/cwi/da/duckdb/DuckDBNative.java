package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;

public class DuckDBNative {
	static {
		System.loadLibrary("duckdb_java");
	}
	// We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers back and forth
	
	/* NB: if you change anything below, run `javah` on this class to re-generate the C header */
	
	// results db_ref database reference object
	protected static native ByteBuffer duckdb_jdbc_startup(String path, boolean read_only);
	protected static native void duckdb_jdbc_shutdown(ByteBuffer db_ref);
	// returns conn_ref connection reference object
	protected static native ByteBuffer duckdb_jdbc_connect(ByteBuffer db_ref);
	protected static native void duckdb_jdbc_disconnect(ByteBuffer conn_ref);
	// returns stmt_ref result reference object
	protected static native ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, String query);
	// returns res_ref result reference object
	protected static native ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params);
	protected static native void duckdb_jdbc_release(ByteBuffer stmt_ref);
	protected static native DuckDBResultSetMetaData duckdb_jdbc_meta(ByteBuffer res_ref);
	protected static native void duckdb_jdbc_free_result(ByteBuffer res_ref);
	protected static native DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref);
}
