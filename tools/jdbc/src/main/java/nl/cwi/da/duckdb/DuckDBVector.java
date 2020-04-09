package nl.cwi.da.duckdb;

import java.nio.ByteBuffer;

public class DuckDBVector {
	
	public DuckDBVector(String duckdb_type, int length,  boolean[] nullmask) {
		super();
		this.duckdb_type = duckdb_type;
		this.length = length;
		this.nullmask = nullmask;
	}
	protected String duckdb_type;
	protected int length;
	protected boolean[] nullmask;
	protected ByteBuffer constlen_data = null;
	protected Object[] varlen_data = null;

}
