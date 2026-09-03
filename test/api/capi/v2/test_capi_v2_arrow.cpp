#include "test_capi_v2.hpp"

#include <cstring>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 Arrow C Data Interface tests.
//
// The chunk converters take a context, which only a callback has, so the value
// round-trips run through two harnesses registered on the connection:
// arrow_roundtrip(x), a scalar function pushing its argument through
// chunk -> Arrow array -> chunk, and arrow_roundtrip_range(n), a table function
// building its own multi-column chunks and round-tripping each one. A value
// survives iff arrow_roundtrip(x) IS NOT DISTINCT FROM x, so the assertions are
// ordinary SQL.
//
// Callbacks must not use Catch assertions: a REQUIRE would throw through the C
// boundary into the engine. They populate the error slot and return instead, and
// the failure surfaces as a query error.
//
// The Arrow structs come from duckdb_v2.h; this file includes no Arrow header.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {
namespace {

duckdb_v2_identifier_t ArrowIdent(const char *s) {
	return duckdb_v2_identifier_t {s, std::strlen(s)};
}

// Builds a type inside a callback. Returns null after populating the error slot.
duckdb_v2_logical_type_handle ArrowTypeInCallback(duckdb_v2_context_handle context, DUCKDB_V2_LOGICAL_TYPE_ID id,
                                                  duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle type = nullptr;
	if (duckdb_v2_context_create_type_from_id(context, id, nullptr, nullptr, 0, &type, err) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	return type;
}

// ---------------------------------------------------------------------------
// arrow_roundtrip(x): passes its argument straight through the Arrow chunk
// converters and returns it unchanged. Declared with an ANY return; the bind
// callback reads the argument type, matches the return type to it, and resolves
// the converter once. One function therefore exercises the exec-phase
// converters over every type, nested ones included.
// ---------------------------------------------------------------------------

void ArrowRtDestroyConverter(void *ptr) {
	auto converter = reinterpret_cast<duckdb_v2_arrow_converter_handle>(ptr);
	duckdb_v2_arrow_converter_destroy(&converter);
}

void ArrowRtBind(duckdb_v2_scalar_function_bind_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_logical_type_handle arg_type = nullptr;
	if (duckdb_v2_scalar_function_bind_get_arg_type(info, 0, &arg_type, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// The result type follows the argument type.
	auto rc = duckdb_v2_scalar_function_bind_set_return_type(info, arg_type, err);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_destroy(&arg_type);
		return;
	}
	// Resolve the one-column Arrow schema for this type into a converter.
	ArrowSchema schema {};
	auto name = Convert("x");
	rc = duckdb_v2_logical_types_to_arrow_schema(context, &arg_type, &name, 1, &schema, err);
	duckdb_v2_logical_type_destroy(&arg_type);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_arrow_converter_handle converter = nullptr;
	rc = duckdb_v2_arrow_converter_create(context, &schema, &converter, err);
	schema.release(&schema);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// The converter rides the bind data and dies with the binding.
	duckdb_v2_opaque bind_data = {converter, ArrowRtDestroyConverter, nullptr};
	duckdb_v2_scalar_function_bind_set_bind_data(info, &bind_data, err);
}

void ArrowRtExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
                 duckdb_v2_error_info_handle *err) {
	void *bind_data = nullptr;
	duckdb_v2_vector_handle arg = nullptr;
	duckdb_v2_vector_handle result = nullptr;
	idx_t count = 0;
	if (duckdb_v2_scalar_function_exec_get_bind_data(info, &bind_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_arg(info, 0, &arg, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_result(info, &result, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_row_count(info, &count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto converter = reinterpret_cast<duckdb_v2_arrow_converter_handle>(bind_data);

	// Wrap the argument vector in a one-column chunk, which is what the exporter takes.
	duckdb_v2_logical_type_handle arg_type = nullptr;
	if (duckdb_v2_vector_get_logical_type(arg, &arg_type, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_data_chunk_handle input = nullptr;
	auto rc = duckdb_v2_data_chunk_create(&arg_type, 1, &input, err);
	duckdb_v2_logical_type_destroy(&arg_type);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_handle input_vector = nullptr;
	if (duckdb_v2_data_chunk_get_vector(input, 0, &input_vector, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_reference(input_vector, arg, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_set_size(input_vector, count, err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_destroy(&input);
		return;
	}

	ArrowArray array {};
	rc = duckdb_v2_data_chunk_to_arrow_array(context, input, &array, err);
	duckdb_v2_data_chunk_destroy(&input);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_data_chunk_handle imported = nullptr;
	rc = duckdb_v2_arrow_converter_array_to_chunk(context, &array, converter, &imported, err);
	// The import adopts the array and nulls its release, so this only fires when the import
	// failed before taking ownership.
	if (array.release) {
		array.release(&array);
	}
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	// Reference the re-imported column into the result: zero-copy, and the shared buffers keep
	// the data alive after the imported chunk goes away.
	duckdb_v2_vector_handle imported_vector = nullptr;
	if (duckdb_v2_data_chunk_get_vector(imported, 0, &imported_vector, err) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_vector_reference(result, imported_vector, err);
	}
	duckdb_v2_data_chunk_destroy(&imported);
}

void RegisterArrowRoundtrip(duckdb_v2_connection_handle conn) {
	duckdb_v2_scalar_function_handle function = nullptr;
	REQUIRE(duckdb_v2_scalar_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("arrow_roundtrip");
	REQUIRE(duckdb_v2_scalar_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto any = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_ANY);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_scalar_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, ArrowIdent("x"), any, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, any, nullptr) == DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_scalar_function_set_bind_callback(function, ArrowRtBind, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_set_exec_callback(function, ArrowRtExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_scalar_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&any);
}

// ---------------------------------------------------------------------------
// arrow_roundtrip_range(n): emits n rows of (i BIGINT, s VARCHAR) where
// i = row index and s = 'r' || i, every chunk having gone through the Arrow
// converters first. This drives the structural edges the scalar harness cannot
// reach: multi-column import, and an array longer than one vector.
// ---------------------------------------------------------------------------

struct ArrowRangeBind {
	int64_t count = 0;
	duckdb_v2_arrow_converter_handle converter = nullptr;
};
struct ArrowRangeGlobal {
	int64_t emitted = 0;
};

void ArrowRangeDestroyBind(void *ptr) {
	auto *bind = static_cast<ArrowRangeBind *>(ptr);
	duckdb_v2_arrow_converter_destroy(&bind->converter);
	delete bind;
}
void ArrowRangeDestroyGlobal(void *ptr) {
	delete static_cast<ArrowRangeGlobal *>(ptr);
}

void ArrowRangeBindCb(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_table_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int64_t count = 0;
	auto rc = duckdb_v2_value_get_bigint(value, &count, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	auto bigint = ArrowTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	auto varchar = ArrowTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, err);
	if (!bigint || !varchar) {
		duckdb_v2_logical_type_destroy(&bigint);
		duckdb_v2_logical_type_destroy(&varchar);
		return;
	}
	duckdb_v2_table_function_bind_add_result_column(info, ArrowIdent("i"), bigint, err);
	duckdb_v2_table_function_bind_add_result_column(info, ArrowIdent("s"), varchar, err);

	// Resolve the two-column schema once, for every chunk this scan will produce.
	duckdb_v2_logical_type_handle types[2] = {bigint, varchar};
	duckdb_v2_str names[2] = {Convert("i"), Convert("s")};
	ArrowSchema schema {};
	rc = duckdb_v2_logical_types_to_arrow_schema(context, types, names, 2, &schema, err);
	duckdb_v2_logical_type_destroy(&bigint);
	duckdb_v2_logical_type_destroy(&varchar);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_arrow_converter_handle converter = nullptr;
	rc = duckdb_v2_arrow_converter_create(context, &schema, &converter, err);
	schema.release(&schema);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	auto *bind = new ArrowRangeBind {count, converter};
	duckdb_v2_opaque bind_data = {bind, ArrowRangeDestroyBind, nullptr};
	duckdb_v2_table_function_bind_set_bind_data(info, &bind_data, err);
}

void ArrowRangeInitCb(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle,
                      duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque state = {new ArrowRangeGlobal(), ArrowRangeDestroyGlobal, nullptr};
	duckdb_v2_table_function_init_global_set_global_state(info, &state, err);
}

void ArrowRangeExecCb(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle context,
                      duckdb_v2_error_info_handle *err) {
	void *raw_bind = nullptr;
	void *raw_global = nullptr;
	duckdb_v2_data_chunk_handle output = nullptr;
	if (duckdb_v2_table_function_exec_get_bind_data(info, &raw_bind, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_global_state(info, &raw_global, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &output, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<ArrowRangeBind *>(raw_bind);
	auto &global = *static_cast<ArrowRangeGlobal *>(raw_global);

	duckdb_v2_vector_handle out_i = nullptr;
	duckdb_v2_vector_handle out_s = nullptr;
	if (duckdb_v2_data_chunk_get_vector(output, 0, &out_i, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(output, 1, &out_s, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (global.emitted >= bind.count) {
		duckdb_v2_vector_set_size(out_i, 0, err);
		return;
	}
	auto remaining = static_cast<idx_t>(bind.count - global.emitted);
	auto rows = remaining < STANDARD_VECTOR_SIZE ? remaining : static_cast<idx_t>(STANDARD_VECTOR_SIZE);

	// Build a fresh input chunk and fill it.
	auto bigint = ArrowTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, err);
	auto varchar = ArrowTypeInCallback(context, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, err);
	if (!bigint || !varchar) {
		duckdb_v2_logical_type_destroy(&bigint);
		duckdb_v2_logical_type_destroy(&varchar);
		return;
	}
	duckdb_v2_logical_type_handle types[2] = {bigint, varchar};
	duckdb_v2_data_chunk_handle input = nullptr;
	auto rc = duckdb_v2_data_chunk_create(types, 2, &input, err);
	duckdb_v2_logical_type_destroy(&bigint);
	duckdb_v2_logical_type_destroy(&varchar);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_handle in_i = nullptr;
	duckdb_v2_vector_handle in_s = nullptr;
	int64_t *i_data = nullptr;
	if (duckdb_v2_data_chunk_get_vector(input, 0, &in_i, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_chunk_get_vector(input, 1, &in_s, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(in_i, reinterpret_cast<void **>(&i_data), err) != DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_data_chunk_destroy(&input);
		return;
	}
	for (idx_t row = 0; row < rows; row++) {
		auto value = global.emitted + static_cast<int64_t>(row);
		i_data[row] = value;
		auto text = "r" + std::to_string(value);
		if (V2VectorAssignString(in_s, row, text.c_str(), text.size(), err) != DUCKDB_V2_ERROR_NONE) {
			duckdb_v2_data_chunk_destroy(&input);
			return;
		}
	}
	duckdb_v2_vector_set_size(in_i, rows, err);
	duckdb_v2_vector_set_size(in_s, rows, err);

	ArrowArray array {};
	rc = duckdb_v2_data_chunk_to_arrow_array(context, input, &array, err);
	duckdb_v2_data_chunk_destroy(&input);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_data_chunk_handle imported = nullptr;
	rc = duckdb_v2_arrow_converter_array_to_chunk(context, &array, bind.converter, &imported, err);
	if (array.release) {
		array.release(&array);
	}
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	duckdb_v2_vector_handle imported_i = nullptr;
	duckdb_v2_vector_handle imported_s = nullptr;
	if (duckdb_v2_data_chunk_get_vector(imported, 0, &imported_i, err) == DUCKDB_V2_ERROR_NONE &&
	    duckdb_v2_data_chunk_get_vector(imported, 1, &imported_s, err) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_vector_reference(out_i, imported_i, err);
		duckdb_v2_vector_reference(out_s, imported_s, err);
	}
	duckdb_v2_data_chunk_destroy(&imported);
	duckdb_v2_vector_set_size(out_i, rows, err);
	global.emitted += static_cast<int64_t>(rows);
}

void RegisterArrowRoundtripRange(duckdb_v2_connection_handle conn) {
	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create_with_connection(conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("arrow_roundtrip_range");
	REQUIRE(duckdb_v2_table_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto bigint = MakeType(conn, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_table_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, ArrowIdent("n"), bigint, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, ArrowRangeBindCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_init_global_callback(function, ArrowRangeInitCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_exec_callback(function, ArrowRangeExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&bigint);
}

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

// Exports a query's result as an Arrow stream. The result is consumed.
DUCKDB_V2_ERROR ArrowStreamFor(duckdb_v2_connection_handle conn, const char *sql, idx_t batch_size,
                               ArrowArrayStream *out, duckdb_v2_error_info_handle *err = nullptr) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto rc = duckdb_v2_result_to_arrow_stream(&result, batch_size, out, err);
	// Consumed on every path that reaches the engine.
	REQUIRE(result == nullptr);
	return rc;
}

struct ArrowStreamStats {
	int64_t rows = 0;
	idx_t arrays = 0;
	int64_t first_array_rows = 0;
};

// Drives get_next to exhaustion, releasing each array.
ArrowStreamStats DrainArrowStream(ArrowArrayStream &stream) {
	ArrowStreamStats stats;
	while (true) {
		ArrowArray array {};
		REQUIRE(stream.get_next(&stream, &array) == 0);
		if (!array.release) {
			break;
		}
		if (stats.arrays == 0) {
			stats.first_array_rows = array.length;
		}
		stats.rows += array.length;
		stats.arrays++;
		array.release(&array);
	}
	return stats;
}

// Runs a single-row single-BOOLEAN query. The queries below aggregate with bool_and over a
// non-empty set, so the row is never NULL.
bool ArrowQueryBool(duckdb_v2_connection_handle conn, const char *sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.data != nullptr);
	auto value = reinterpret_cast<const bool *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return value;
}

} // namespace

// ===========================================================================
// Value round-trips: DuckDB chunk -> Arrow array -> DuckDB chunk.
// ===========================================================================

TEST_CASE("V2 arrow: a flat round-trip preserves values", "[capi_v2][arrow]") {
	EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);

	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(i) IS NOT DISTINCT FROM i) "
	                                "FROM range(-100, 100) t(i)"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(d) IS NOT DISTINCT FROM d) "
	                                "FROM (SELECT i * 1.5 AS d FROM range(50) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(s) IS NOT DISTINCT FROM s) "
	                                "FROM (SELECT 'value_' || i AS s FROM range(50) t(i))"));
	// Booleans, dates and blobs travel through their own Arrow layouts.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(b) IS NOT DISTINCT FROM b) "
	                                "FROM (SELECT i % 2 = 0 AS b FROM range(20) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(d) IS NOT DISTINCT FROM d) "
	                                "FROM (SELECT DATE '2020-01-01' + i::INTEGER AS d FROM range(20) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(x) IS NOT DISTINCT FROM x) "
	                                "FROM (SELECT ('blob_' || i)::BLOB AS x FROM range(20) t(i))"));
}

TEST_CASE("V2 arrow: NULLs survive a round-trip", "[capi_v2][arrow]") {
	EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);

	// Every third value is NULL, so validity has to cross both ways.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                                "FROM (SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS v FROM range(60) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                                "FROM (SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE 's' || i END AS v "
	                                "FROM range(60) t(i))"));
	// An all-NULL column, where the array may carry no buffers at all.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NULL) "
	                                "FROM (SELECT NULL::INTEGER AS v FROM range(10) t(i))"));
}

TEST_CASE("V2 arrow: nested values survive a round-trip", "[capi_v2][arrow]") {
	EnvFixture fx;
	RegisterArrowRoundtrip(fx.conn);

	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(l) IS NOT DISTINCT FROM l) "
	                                "FROM (SELECT [i, i + 1, i + 2] AS l FROM range(30) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(s) IS NOT DISTINCT FROM s) "
	                                "FROM (SELECT {'a': i, 'b': 'x' || i} AS s FROM range(30) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(m) IS NOT DISTINCT FROM m) "
	                                "FROM (SELECT MAP {'k' || i: i} AS m FROM range(30) t(i))"));
	// A list of structs, and a struct holding a list with NULLs inside.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                                "FROM (SELECT [{'a': i}, {'a': i + 1}] AS v FROM range(20) t(i))"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(arrow_roundtrip(v) IS NOT DISTINCT FROM v) "
	                                "FROM (SELECT {'l': [i, NULL, i + 2]} AS v FROM range(20) t(i))"));
}

TEST_CASE("V2 arrow: a multi-column round-trip preserves rows", "[capi_v2][arrow]") {
	EnvFixture fx;
	RegisterArrowRoundtripRange(fx.conn);

	// Every row comes back through the converters, so this checks both columns and the
	// row count at once.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT bool_and(s = 'r' || i) FROM arrow_roundtrip_range(100)"));
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT count(*) = 100 AND min(i) = 0 AND max(i) = 99 "
	                                "FROM arrow_roundtrip_range(100)"));
	// More rows than fit in one vector, so several arrays are converted in sequence.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT count(*) = 5000 AND sum(i) = 12497500 "
	                                "FROM arrow_roundtrip_range(5000)"));
	// And the empty case.
	REQUIRE(ArrowQueryBool(fx.conn, "SELECT count(*) = 0 FROM arrow_roundtrip_range(0)"));
}

// ===========================================================================
// result_to_arrow_stream
// ===========================================================================

TEST_CASE("V2 arrow: a stream yields every row and a stable schema", "[capi_v2][arrow]") {
	EnvFixture fx;

	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i, 'r' || i AS s FROM range(1000) t(i)", 0, &stream) ==
	        DUCKDB_V2_ERROR_NONE);

	// The schema is available before a single row is pulled, and each call hands out an
	// independently owned copy.
	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.release != nullptr);
	REQUIRE(schema.n_children == 2);
	REQUIRE(std::string(schema.children[0]->name) == "i");
	REQUIRE(std::string(schema.children[1]->name) == "s");
	schema.release(&schema);

	ArrowSchema again {};
	REQUIRE(stream.get_schema(&stream, &again) == 0);
	REQUIRE(again.n_children == 2);
	again.release(&again);

	auto stats = DrainArrowStream(stream);
	REQUIRE(stats.rows == 1000);
	REQUIRE(stats.arrays >= 1);
	// Exhaustion is idempotent.
	ArrowArray trailing {};
	REQUIRE(stream.get_next(&stream, &trailing) == 0);
	REQUIRE(trailing.release == nullptr);

	stream.release(&stream);
	REQUIRE(stream.release == nullptr);
}

TEST_CASE("V2 arrow: a stream coalesces chunks to batch_size", "[capi_v2][arrow]") {
	EnvFixture fx;

	// batch_size 1 gives one row per array, which pins that the setting is honored rather
	// than ignored in favor of the engine's own chunking.
	ArrowArrayStream single {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i FROM range(7) t(i)", 1, &single) == DUCKDB_V2_ERROR_NONE);
	auto single_stats = DrainArrowStream(single);
	REQUIRE(single_stats.rows == 7);
	REQUIRE(single_stats.arrays == 7);
	REQUIRE(single_stats.first_array_rows == 1);
	single.release(&single);

	// A batch larger than the whole result coalesces it into one array, whatever the engine's
	// vector size is.
	ArrowArrayStream coalesced {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i FROM range(100) t(i)", 4096, &coalesced) == DUCKDB_V2_ERROR_NONE);
	auto coalesced_stats = DrainArrowStream(coalesced);
	REQUIRE(coalesced_stats.rows == 100);
	REQUIRE(coalesced_stats.arrays == 1);
	REQUIRE(coalesced_stats.first_array_rows == 100);
	coalesced.release(&coalesced);
}

TEST_CASE("V2 arrow: a stream over a partially consumed result covers the remainder", "[capi_v2][arrow]") {
	EnvFixture fx;

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT i FROM range(1000) t(i)", &result) == DUCKDB_V2_ERROR_NONE);
	// Take one chunk natively first.
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	idx_t consumed = 0;
	REQUIRE(duckdb_v2_data_chunk_get_size(chunk, &consumed, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(consumed > 0);
	duckdb_v2_data_chunk_destroy(&chunk);

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(&result, 0, &stream, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(result == nullptr);
	auto stats = DrainArrowStream(stream);
	REQUIRE(stats.rows == static_cast<int64_t>(1000 - consumed));
	stream.release(&stream);
}

TEST_CASE("V2 arrow: an empty result gives a schema and no rows", "[capi_v2][arrow]") {
	EnvFixture fx;

	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i, 'x' AS s FROM range(0) t(i)", 0, &stream) == DUCKDB_V2_ERROR_NONE);
	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.n_children == 2);
	schema.release(&schema);
	auto stats = DrainArrowStream(stream);
	REQUIRE(stats.rows == 0);
	REQUIRE(stats.arrays == 0);
	stream.release(&stream);
}

TEST_CASE("V2 arrow: the stream owns the connection until released", "[capi_v2][arrow]") {
	EnvFixture fx;

	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i FROM range(100000) t(i)", 0, &stream) == DUCKDB_V2_ERROR_NONE);

	// The stream took over the result's live-result slot, so the connection is still busy.
	duckdb_v2_result_handle blocked = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &blocked) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(blocked == nullptr);

	// Releasing it frees the connection, even with the stream undrained.
	stream.release(&stream);
	duckdb_v2_result_handle after = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &after) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(after) == 1);
	duckdb_v2_result_destroy(&after);
}

TEST_CASE("V2 arrow: get_schema still works after the stream is drained", "[capi_v2][arrow]") {
	EnvFixture fx;

	// The schema is cached while the transaction is live, so reading it after exhaustion, when
	// the catalog is no longer reachable, must still succeed.
	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i FROM range(10) t(i)", 0, &stream) == DUCKDB_V2_ERROR_NONE);
	auto stats = DrainArrowStream(stream);
	REQUIRE(stats.rows == 10);

	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.n_children == 1);
	REQUIRE(std::string(schema.children[0]->name) == "i");
	schema.release(&schema);
	stream.release(&stream);
}

TEST_CASE("V2 arrow: an ENUM stream carries its dictionary", "[capi_v2][arrow]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");

	// An ENUM's Arrow schema is a dictionary read from the catalog, so this only works because
	// the schema is built while the transaction is live rather than lazily in get_schema.
	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT 'happy'::mood AS m FROM range(4)", 0, &stream) == DUCKDB_V2_ERROR_NONE);
	auto stats = DrainArrowStream(stream);
	REQUIRE(stats.rows == 4);
	ArrowSchema schema {};
	REQUIRE(stream.get_schema(&stream, &schema) == 0);
	REQUIRE(schema.n_children == 1);
	REQUIRE(schema.children[0]->dictionary != nullptr);
	schema.release(&schema);
	stream.release(&stream);
}

TEST_CASE("V2 arrow: a released stream refuses further calls", "[capi_v2][arrow]") {
	EnvFixture fx;

	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn, "SELECT i FROM range(10) t(i)", 0, &stream) == DUCKDB_V2_ERROR_NONE);
	auto get_schema = stream.get_schema;
	auto get_next = stream.get_next;
	auto get_last_error = stream.get_last_error;
	stream.release(&stream);
	REQUIRE(stream.release == nullptr);

	// The callbacks survive the release, and report EINVAL rather than reading freed state.
	ArrowSchema schema {};
	REQUIRE(get_schema(&stream, &schema) == EINVAL);
	ArrowArray array {};
	REQUIRE(get_next(&stream, &array) == EINVAL);
	REQUIRE(array.release == nullptr);
	REQUIRE(std::string(get_last_error(&stream)) == "arrow stream was released");
}

TEST_CASE("V2 arrow: an execution error surfaces from the stream", "[capi_v2][arrow]") {
	EnvFixture fx;

	// The error is raised mid-scan, so it can only surface from get_next, which reports it
	// through get_last_error rather than an error code.
	ArrowArrayStream stream {};
	REQUIRE(ArrowStreamFor(fx.conn,
	                       "SELECT CASE WHEN i = 500 THEN error('boom') ELSE i::VARCHAR END "
	                       "FROM range(1000) t(i)",
	                       0, &stream) == DUCKDB_V2_ERROR_NONE);
	int rc = 0;
	bool exhausted = false;
	while (rc == 0) {
		ArrowArray array {};
		rc = stream.get_next(&stream, &array);
		if (rc == 0 && !array.release) {
			exhausted = true;
			break;
		}
		if (array.release) {
			array.release(&array);
		}
	}
	REQUIRE_FALSE(exhausted);
	REQUIRE(rc != 0);
	REQUIRE(std::string(stream.get_last_error(&stream)).find("boom") != std::string::npos);
	stream.release(&stream);

	// The failed stream still frees the connection when released.
	duckdb_v2_result_handle after = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &after) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(after) == 1);
	duckdb_v2_result_destroy(&after);
}

// ===========================================================================
// Reading a converter back, and argument rejection.
// ===========================================================================

TEST_CASE("V2 arrow: a converter reports the DuckDB shape of an Arrow schema", "[capi_v2][arrow]") {
	EnvFixture fx;
	// Registered as a scalar function purely to reach a context; what it observes is latched
	// and asserted after the query.
	static idx_t observed_count = 0;
	static std::string observed_names;
	static DUCKDB_V2_LOGICAL_TYPE_ID observed_first = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;

	observed_count = 0;
	observed_names.clear();
	observed_first = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;

	auto exec = [](duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
	               duckdb_v2_error_info_handle *err) {
		duckdb_v2_vector_handle result = nullptr;
		if (duckdb_v2_scalar_function_exec_get_result(info, &result, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_logical_type_handle bigint = nullptr;
		duckdb_v2_logical_type_handle varchar = nullptr;
		if (duckdb_v2_context_create_type_from_id(context, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, 0,
		                                          &bigint, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_context_create_type_from_id(context, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, nullptr, nullptr, 0,
		                                          &varchar, err) != DUCKDB_V2_ERROR_NONE) {
			duckdb_v2_logical_type_destroy(&bigint);
			duckdb_v2_logical_type_destroy(&varchar);
			return;
		}
		duckdb_v2_logical_type_handle types[2] = {bigint, varchar};
		duckdb_v2_str names[2] = {duckdb_v2_str {"a", 1}, duckdb_v2_str {"b", 1}};
		ArrowSchema schema {};
		auto rc = duckdb_v2_logical_types_to_arrow_schema(context, types, names, 2, &schema, err);
		duckdb_v2_logical_type_destroy(&bigint);
		duckdb_v2_logical_type_destroy(&varchar);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		// Round the schema back through a converter: the DuckDB shape it reports should be the
		// types we started from.
		duckdb_v2_arrow_converter_handle converter = nullptr;
		rc = duckdb_v2_arrow_converter_create(context, &schema, &converter, err);
		schema.release(&schema);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_schema_handle resolved = nullptr;
		rc = duckdb_v2_arrow_converter_get_schema(converter, &resolved, err);
		duckdb_v2_arrow_converter_destroy(&converter);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_schema_get_count(resolved, &observed_count, err);
		for (idx_t i = 0; i < observed_count; i++) {
			duckdb_v2_str field_name = {nullptr, 0};
			duckdb_v2_logical_type_handle field_type = nullptr;
			if (duckdb_v2_schema_get_field(resolved, i, &field_name, &field_type, err) != DUCKDB_V2_ERROR_NONE) {
				break;
			}
			observed_names.append(field_name.ptr, field_name.len);
			if (i == 0) {
				duckdb_v2_logical_type_get_id(field_type, &observed_first, err);
			}
		}
		duckdb_v2_schema_destroy(&resolved);
		int32_t one = 1;
		void *data = nullptr;
		if (duckdb_v2_vector_get_data_mutable(result, &data, err) == DUCKDB_V2_ERROR_NONE) {
			std::memcpy(data, &one, sizeof(one));
		}
	};

	duckdb_v2_scalar_function_handle function = nullptr;
	REQUIRE(duckdb_v2_scalar_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Convert("probe_converter");
	REQUIRE(duckdb_v2_scalar_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_scalar_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, ArrowIdent("x"), integer, nullptr, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_set_exec_callback(function, exec, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_scalar_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&integer);

	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT probe_converter(1)", &result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(result) == 1);
	duckdb_v2_result_destroy(&result);

	REQUIRE(observed_count == 2);
	REQUIRE(observed_names == "ab");
	REQUIRE(observed_first == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
}

TEST_CASE("V2 arrow: functions guard null arguments", "[capi_v2][arrow]") {
	EnvFixture fx;

	ArrowArrayStream stream {};
	REQUIRE(duckdb_v2_result_to_arrow_stream(nullptr, 0, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_result_handle empty = nullptr;
	REQUIRE(duckdb_v2_result_to_arrow_stream(&empty, 0, &stream, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// A rejection must leave the result usable, since it never reached the engine.
	duckdb_v2_result_handle live = nullptr;
	REQUIRE(Query(fx.conn, "SELECT 1", &live) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_result_to_arrow_stream(&live, 0, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(live != nullptr);
	REQUIRE(DrainRowCount(live) == 1);
	duckdb_v2_result_destroy(&live);

	ArrowSchema schema {};
	duckdb_v2_str name = Convert("a");
	REQUIRE(duckdb_v2_logical_types_to_arrow_schema(nullptr, nullptr, &name, 0, &schema, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);

	ArrowArray array {};
	REQUIRE(duckdb_v2_data_chunk_to_arrow_array(nullptr, nullptr, &array, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	duckdb_v2_arrow_converter_handle converter = nullptr;
	REQUIRE(duckdb_v2_arrow_converter_create(nullptr, &schema, &converter, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(converter == nullptr);

	duckdb_v2_data_chunk_handle chunk = nullptr;
	REQUIRE(duckdb_v2_arrow_converter_array_to_chunk(nullptr, &array, nullptr, &chunk, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(chunk == nullptr);

	duckdb_v2_schema_handle resolved = nullptr;
	REQUIRE(duckdb_v2_arrow_converter_get_schema(nullptr, &resolved, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(resolved == nullptr);

	// Destroy is null-safe and idempotent.
	REQUIRE(duckdb_v2_arrow_converter_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_arrow_converter_destroy(&converter) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(converter == nullptr);
	REQUIRE(duckdb_v2_arrow_converter_destroy(&converter) == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
