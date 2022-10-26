package org.duckdb;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DuckDBArrowResult {

	private Class<?> vector_root_class;
	private Class<?> schema_class;
	private Class<?> dictionary_class;

	private Class<?> root_allocator_class;
	private Class<?> buffer_allocator_class;

	private Class<?> c_schema_class;
	private Class<?> c_array_class;
	private Class<?> c_data_class;
	private Class<?> c_dictionary_class;

	private Object allocator;

	private DuckDBResultSet result_set;
	private Object schema;
	private Object vector_schema_root;

	public DuckDBArrowResult(ResultSet result_set_p) throws Exception {
		if (!(result_set_p instanceof DuckDBResultSet)) {
			throw new SQLException("This only works with a DuckDB ResultSet");
		}
		result_set = (DuckDBResultSet) result_set_p;

		vector_root_class = Class.forName("org.apache.arrow.vector.VectorSchemaRoot");
		schema_class = Class.forName("org.apache.arrow.vector.types.pojo.Schema");
		dictionary_class = Class.forName("org.apache.arrow.vector.dictionary.DictionaryProvider");

		root_allocator_class = Class.forName("org.apache.arrow.memory.RootAllocator");
		buffer_allocator_class = Class.forName("org.apache.arrow.memory.BufferAllocator");

		c_schema_class = Class.forName("org.apache.arrow.c.ArrowSchema");
		c_array_class = Class.forName("org.apache.arrow.c.ArrowArray");
		c_data_class = Class.forName("org.apache.arrow.c.Data");
		c_dictionary_class = Class.forName("org.apache.arrow.c.CDataDictionaryProvider");

		allocator = root_allocator_class.getConstructor().newInstance();

		Object c_schema = c_schema_class.getMethod("allocateNew", buffer_allocator_class).invoke(null, allocator);
		result_set.arrowExportSchema((Long) c_schema_class.getMethod("memoryAddress").invoke(c_schema));
		schema = c_data_class.getMethod("importSchema", buffer_allocator_class, c_schema_class, c_dictionary_class)
				.invoke(null, allocator, c_schema, null);
		vector_schema_root = vector_root_class.getMethod("create", schema_class, buffer_allocator_class).invoke(null,
				schema, allocator);
	}

	public Object fetchVectorSchemaRoot() throws Exception {
		Object c_array = c_array_class.getMethod("allocateNew", buffer_allocator_class).invoke(null, allocator);
		Long c_array_address = (Long) c_array_class.getMethod("memoryAddress").invoke(c_array);

		if (!result_set.arrowFetch(c_array_address)) {
			return null;
		}

		c_data_class.getMethod("importIntoVectorSchemaRoot", buffer_allocator_class, c_array_class, vector_root_class,
				dictionary_class).invoke(null, allocator, c_array, vector_schema_root, null);

		return vector_schema_root;
	}
}
