package nl.cwi.da.duckdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class DuckDBNative {
	static {
		String libName = "libduckdb_java.so";
		try {
			Path libFile = Files.createTempFile("libduckdb_java", ".so");
			InputStream in = DuckDBNative.class.getResource("/" + libName).openStream();
			Files.copy(in, libFile, StandardCopyOption.REPLACE_EXISTING);
			new File(libFile.toString()).deleteOnExit();
			System.load(libFile.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}

//		System.loadLibrary("duckdb_java");
	}
	// We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers
	// back and forth

	/*
	 * NB: if you change anything below, run `javah` on this class to re-generate
	 * the C header
	 */

	// results db_ref database reference object
	protected static native ByteBuffer duckdb_jdbc_startup(byte[] path, boolean read_only);

	protected static native void duckdb_jdbc_shutdown(ByteBuffer db_ref);

	// returns conn_ref connection reference object
	protected static native ByteBuffer duckdb_jdbc_connect(ByteBuffer db_ref);

	protected static native void duckdb_jdbc_set_auto_commit(ByteBuffer conn_ref, boolean auto_commit);

	protected static native boolean duckdb_jdbc_get_auto_commit(ByteBuffer conn_ref);

	protected static native void duckdb_jdbc_disconnect(ByteBuffer conn_ref);

	// returns stmt_ref result reference object
	protected static native ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, byte[] query);

	protected static native String duckdb_jdbc_prepare_type(ByteBuffer stmt_ref);

	protected static native void duckdb_jdbc_release(ByteBuffer stmt_ref);

	protected static native DuckDBResultSetMetaData duckdb_jdbc_meta(ByteBuffer stmt_ref);

	
	// returns res_ref result reference object
	protected static native ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params);


	protected static native void duckdb_jdbc_free_result(ByteBuffer res_ref);

	protected static native DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref);
	
	protected static native int duckdb_jdbc_fetch_size();
}
