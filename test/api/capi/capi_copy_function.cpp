#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

//----------------------------------------------------------------------------------------------------------------------
// COPY (...) TO (...)
//----------------------------------------------------------------------------------------------------------------------

struct MyCopyFunctionBindData {
	idx_t max_size;
	idx_t min_size;
};

struct MyCopyFunctionGlobalState {
	int64_t total_written_bytes = 0;
};

static void MyCopyFunctionBind(duckdb_bind_info info) {

	auto max = duckdb_copy_function_to_bind_get_option(info, "max_size");
	if (!max) {
		// Set default, if not given
		max = duckdb_create_int64(1000);
	}
	auto min = duckdb_copy_function_to_bind_get_option(info, "min_size");
	if (!min) {
		// Set default, if not given
		max = duckdb_create_int64(100);
	}

	if (duckdb_get_int64(max) < 0) {
		duckdb_destroy_value(&max);
		duckdb_destroy_value(&min);
		duckdb_copy_function_to_bind_set_error(info, "max_size must be >= 0");
		return;
	}

	if (duckdb_get_int64(min) < 0) {
		duckdb_destroy_value(&max);
		duckdb_destroy_value(&min);
		duckdb_copy_function_to_bind_set_error(info, "min_size must be >= 0");
		return;
	}

	// Now inspect the input columns
	auto column_count = duckdb_copy_function_to_bind_get_column_count(info);
	if (column_count != 1) {
		duckdb_destroy_value(&max);
		duckdb_destroy_value(&min);
		duckdb_copy_function_to_bind_set_error(info, "Expected exactly one column");
		return;
	}

	auto column_type = duckdb_copy_function_to_bind_get_column_type(info, 0);
	if (duckdb_get_type_id(column_type) != DUCKDB_TYPE_BLOB) {
		duckdb_destroy_value(&max);
		duckdb_destroy_value(&min);
		duckdb_destroy_logical_type(&column_type);
		duckdb_copy_function_to_bind_set_error(info, "Expected column of type BLOB");
		return;
	}

	auto my_bind_data = new MyCopyFunctionBindData();
	my_bind_data->max_size = duckdb_get_int64(max);
	my_bind_data->min_size = duckdb_get_int64(min);

	duckdb_copy_function_to_bind_set_bind_data(info, my_bind_data, [](void* bind_data) {
		auto my_bind_data = (MyCopyFunctionBindData*)bind_data;
		delete my_bind_data;
	});
}


static void MyCopyFunctionInit(duckdb_init_info info) {
	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_to_global_init_get_bind_data(info);

	if (bind_data->min_size == 42) {
		// Ooops, forgot to check this in the bind!
		duckdb_copy_function_to_global_init_set_error(info, "My bad, min_size cannot be 42!");
		return;
	}

	// Initialize state
	auto g_state = new MyCopyFunctionGlobalState();
	g_state->total_written_bytes = 0;

	duckdb_copy_function_to_global_init_set_global_state(info, g_state, [](void* state) {
		auto g_state = (MyCopyFunctionGlobalState*)state;
		delete g_state;
	} );
}

static void MyCopyFunctionSink(duckdb_copy_to_sink_info info, duckdb_data_chunk input) {
	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_to_sink_get_bind_data(info);
	auto g_state = (MyCopyFunctionGlobalState *)duckdb_copy_function_to_sink_get_global_state(info);

	// Sink the data
	auto row_count = duckdb_data_chunk_get_size(input);
	auto col_vec = duckdb_data_chunk_get_vector(input, 0);
	auto col_data = (duckdb_string_t *)duckdb_vector_get_data(col_vec);

	for (idx_t r = 0; r < row_count; r++) {
		auto len = duckdb_string_t_length(col_data[r]);
		g_state->total_written_bytes += len;
		if (g_state->total_written_bytes > bind_data->max_size) {
			duckdb_copy_function_to_sink_set_error(info, "Wrote too much data");
			return;
		}
	}
}

static void MyCopyFunctionFinalize(duckdb_copy_to_finalize_info info) {
	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_to_finalize_get_bind_data(info);
	auto g_state = (MyCopyFunctionGlobalState *)duckdb_copy_function_to_finalize_get_global_state(info);

	// Check that we actually wrote enough!
	if (g_state->total_written_bytes < bind_data->min_size) {
		duckdb_copy_function_to_finalize_set_error(info, "Wrote too little data");
		return;
	}
}

//----------------------------------------------------------------------------------------------------------------------
// COPY (...) FROM (...)
//----------------------------------------------------------------------------------------------------------------------
// TODO: implement

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("Test Copy Functions in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_copy_function func = duckdb_create_copy_function();
	REQUIRE(func != nullptr);

	// COPY (...) TO (...)
	duckdb_copy_function_set_name(func, "my_copy");
	duckdb_copy_function_to_set_bind(func, MyCopyFunctionBind);
	duckdb_copy_function_to_set_global_init(func, MyCopyFunctionInit);
	duckdb_copy_function_to_set_sink(func, MyCopyFunctionSink);
	duckdb_copy_function_to_set_finalize(func, MyCopyFunctionFinalize);

	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_copy_function_to_add_option(func, "max_size", bigint_type, "The max size of the written blob");
	duckdb_copy_function_to_add_option(func, "min_size", bigint_type, "The min size of the written blob");
	duckdb_destroy_logical_type(&bigint_type);

	auto status = duckdb_register_copy_function(tester.connection, func);
	REQUIRE(status == DuckDBSuccess);

	result = tester.Query("SELECT * FROM my_function(1)");
}
