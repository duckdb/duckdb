package org.duckdb;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.Properties;

public class DuckDBNative {
    static {
        try {
            String os_name = "";
            String os_arch;
            String os_name_detect = System.getProperty("os.name").toLowerCase().trim();
            String os_arch_detect = System.getProperty("os.arch").toLowerCase().trim();
            switch (os_arch_detect) {
            case "x86_64":
            case "amd64":
                os_arch = "amd64";
                break;
            case "aarch64":
            case "arm64":
                os_arch = "arm64";
                break;
            case "i386":
                os_arch = "i386";
                break;
            default:
                throw new IllegalStateException("Unsupported system architecture");
            }
            if (os_name_detect.startsWith("windows")) {
                os_name = "windows";
            } else if (os_name_detect.startsWith("mac")) {
                os_name = "osx";
                os_arch = "universal";
            } else if (os_name_detect.startsWith("linux")) {
                os_name = "linux";
            }
            String lib_res_name = "/libduckdb_java.so"
                                  + "_" + os_name + "_" + os_arch;

            Path lib_file = Files.createTempFile("libduckdb_java", ".so");
            URL lib_res = DuckDBNative.class.getResource(lib_res_name);
            if (lib_res == null) {
                throw new IOException(lib_res_name + " not found");
            }
            Files.copy(lib_res.openStream(), lib_file, StandardCopyOption.REPLACE_EXISTING);
            new File(lib_file.toString()).deleteOnExit();
            System.load(lib_file.toAbsolutePath().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers
    // back and forth

    /*
     * NB: if you change anything below, run `javah` on this class to re-generate
     * the C header. CMake does this as well
     */

    // results ConnectionHolder reference object
    protected static native ByteBuffer duckdb_jdbc_startup(byte[] path, boolean read_only, Properties props)
        throws SQLException;

    // returns conn_ref connection reference object
    protected static native ByteBuffer duckdb_jdbc_connect(ByteBuffer db_ref) throws SQLException;

    protected static native void duckdb_jdbc_set_auto_commit(ByteBuffer conn_ref, boolean auto_commit)
        throws SQLException;

    protected static native boolean duckdb_jdbc_get_auto_commit(ByteBuffer conn_ref) throws SQLException;

    protected static native void duckdb_jdbc_disconnect(ByteBuffer conn_ref);

    protected static native void duckdb_jdbc_set_schema(ByteBuffer conn_ref, String schema);

    protected static native void duckdb_jdbc_set_catalog(ByteBuffer conn_ref, String catalog);

    protected static native String duckdb_jdbc_get_schema(ByteBuffer conn_ref);

    protected static native String duckdb_jdbc_get_catalog(ByteBuffer conn_ref);

    // returns stmt_ref result reference object
    protected static native ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, byte[] query) throws SQLException;

    protected static native void duckdb_jdbc_release(ByteBuffer stmt_ref);

    protected static native DuckDBResultSetMetaData duckdb_jdbc_query_result_meta(ByteBuffer result_ref)
        throws SQLException;

    protected static native DuckDBResultSetMetaData duckdb_jdbc_prepared_statement_meta(ByteBuffer stmt_ref)
        throws SQLException;

    // returns res_ref result reference object
    protected static native ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params) throws SQLException;

    protected static native void duckdb_jdbc_free_result(ByteBuffer res_ref);

    protected static native DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref, ByteBuffer conn_ref)
        throws SQLException;

    protected static native int duckdb_jdbc_fetch_size();

    protected static native long duckdb_jdbc_arrow_stream(ByteBuffer res_ref, long batch_size);

    protected static native void duckdb_jdbc_arrow_register(ByteBuffer conn_ref, long arrow_array_stream_pointer,
                                                            byte[] name);

    protected static native ByteBuffer duckdb_jdbc_create_appender(ByteBuffer conn_ref, byte[] schema_name,
                                                                   byte[] table_name) throws SQLException;

    protected static native void duckdb_jdbc_appender_begin_row(ByteBuffer appender_ref) throws SQLException;

    protected static native void duckdb_jdbc_appender_end_row(ByteBuffer appender_ref) throws SQLException;

    protected static native void duckdb_jdbc_appender_flush(ByteBuffer appender_ref) throws SQLException;

    protected static native void duckdb_jdbc_interrupt(ByteBuffer conn_ref);

    protected static native void duckdb_jdbc_appender_close(ByteBuffer appender_ref) throws SQLException;

    protected static native void duckdb_jdbc_appender_append_boolean(ByteBuffer appender_ref, boolean value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_byte(ByteBuffer appender_ref, byte value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_short(ByteBuffer appender_ref, short value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_int(ByteBuffer appender_ref, int value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_long(ByteBuffer appender_ref, long value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_float(ByteBuffer appender_ref, float value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_double(ByteBuffer appender_ref, double value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_string(ByteBuffer appender_ref, byte[] value)
        throws SQLException;

    protected static native void duckdb_jdbc_appender_append_null(ByteBuffer appender_ref) throws SQLException;

    protected static native void duckdb_jdbc_create_extension_type(ByteBuffer conn_ref) throws SQLException;

    public static void duckdb_jdbc_create_extension_type(DuckDBConnection conn) throws SQLException {
        duckdb_jdbc_create_extension_type(conn.conn_ref);
    }
}
