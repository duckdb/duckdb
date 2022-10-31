package org.duckdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class DuckDBDatabase {

	protected String url;
	protected boolean read_only;
	private AtomicInteger connections;

	public DuckDBDatabase(String url, boolean read_only, Properties props) throws SQLException {
		if (!url.startsWith("jdbc:duckdb")) {
			throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
		}
		this.url = url;
		String db_dir = url.replaceFirst("^jdbc:duckdb:", "").trim();
		if (db_dir.length() == 0) {
			db_dir = ":memory:";
		}
		this.read_only = read_only;
		db_ref = DuckDBNative.duckdb_jdbc_startup(db_dir.getBytes(StandardCharsets.UTF_8), read_only, props);
		connections = new AtomicInteger();
	}

	public void shutdown() {
		try {
			finalize();
		} catch (Throwable e) {
		}
	}

	protected synchronized void finalize() throws Throwable {
		if (db_ref != null) {
			DuckDBNative.duckdb_jdbc_shutdown(db_ref);
			db_ref = null;
		}
	}

	protected ByteBuffer db_ref;

	void incrementConnections() {
		this.connections.incrementAndGet();
	}
	void maybeShutdown() {
		if (this.connections.decrementAndGet() == 0) {
			this.shutdown();
		}
	}

	public boolean isShutdown() {
		return db_ref == null;
	}
}
