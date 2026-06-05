#include "capi_tester.hpp"
#include "duckdb.h"

#include <cstring> // for strcmp

using namespace duckdb;
using namespace std;

//----------------------------------------------------------------------------------------------------------------------
// COPY (...) TO (...)
//----------------------------------------------------------------------------------------------------------------------

struct MyCopyFunctionExtraInfo {
	idx_t illegal_min_value = 42;
};

struct MyCopyFunctionBindData {
	idx_t max_size;
	idx_t min_size;
};

struct MyCopyFunctionGlobalState {
	idx_t total_written_bytes = 0;
	duckdb_file_system file_system = nullptr;
	duckdb_file_handle file_handle = nullptr;

	~MyCopyFunctionGlobalState() {
		if (file_handle) {
			duckdb_destroy_file_handle(&file_handle);
		}
		if (file_system) {
			duckdb_destroy_file_system(&file_system);
		}
	}
};

static void MyCopyFunctionBind(duckdb_copy_function_bind_info info) {
	// COVERAGE
	duckdb_copy_function_bind_set_error(nullptr, "foo");
	duckdb_copy_function_bind_set_error(info, nullptr);
	duckdb_copy_function_bind_set_error(nullptr, nullptr);
	REQUIRE(duckdb_copy_function_bind_get_column_count(nullptr) == 0);
	REQUIRE(duckdb_copy_function_bind_get_column_type(nullptr, 0) == nullptr);
	REQUIRE(duckdb_copy_function_bind_get_column_type(info, 9999) == nullptr);
	REQUIRE(duckdb_copy_function_bind_get_options(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_bind_get_client_context(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_bind_get_extra_info(nullptr) == nullptr);

	auto options = duckdb_copy_function_bind_get_options(info);
	if (!options) {
		duckdb_copy_function_bind_set_error(info, "No options given!");
		return;
	}

	// Extract options
	auto options_type = duckdb_get_value_type(options);
	if (duckdb_get_type_id(options_type) != DUCKDB_TYPE_STRUCT) {
		duckdb_destroy_value(&options);
		duckdb_copy_function_bind_set_error(info, "No options given!");
		return;
	}
	auto struct_size = duckdb_struct_type_child_count(options_type);
	if (struct_size > 2) {
		duckdb_destroy_value(&options);
		duckdb_copy_function_bind_set_error(info, "Too many options given!");
		return;
	}

	// Extract max_size and min_size
	int32_t min_size = 0;
	int32_t max_size = 0;

	for (idx_t i = 0; i < struct_size; i++) {
		auto child_name = duckdb_struct_type_child_name(options_type, i);
		auto child_value = duckdb_get_struct_child(options, i);
		auto child_type = duckdb_get_value_type(child_value);
		if (duckdb_get_type_id(child_type) != DUCKDB_TYPE_INTEGER) {
			duckdb_destroy_value(&options);
			duckdb_destroy_value(&child_value);
			duckdb_free(child_name);
			duckdb_copy_function_bind_set_error(info, "Options must be of type INT");
			return;
		}
		if (strcmp(child_name, "MAX_SIZE") == 0) {
			max_size = duckdb_get_int32(child_value);
		} else if (strcmp(child_name, "MIN_SIZE") == 0) {
			min_size = duckdb_get_int32(child_value);
		} else {
			duckdb_destroy_value(&options);
			duckdb_destroy_value(&child_value);
			duckdb_free(child_name);
			duckdb_copy_function_bind_set_error(info, "Unknown option given");
			return;
		}
		duckdb_free(child_name);
		duckdb_destroy_value(&child_value);
	}

	if (max_size < 0) {
		duckdb_destroy_value(&options);
		duckdb_copy_function_bind_set_error(info, "MAX_SIZE must be >= 0");
		return;
	}
	if (min_size < 0) {
		duckdb_destroy_value(&options);
		duckdb_copy_function_bind_set_error(info, "MIN_SIZE must be >= 0");
		return;
	}

	// Now were done with options, destroy it!
	duckdb_destroy_value(&options);

	// Now inspect the input columns
	auto column_count = duckdb_copy_function_bind_get_column_count(info);
	if (column_count != 1) {
		duckdb_copy_function_bind_set_error(info, "Expected exactly one column");
		return;
	}

	auto column_type = duckdb_copy_function_bind_get_column_type(info, 0);
	if (duckdb_get_type_id(column_type) != DUCKDB_TYPE_BIGINT) {
		duckdb_copy_function_bind_set_error(info, "Expected column of type BIGINT");
		duckdb_destroy_logical_type(&column_type);
		return;
	}

	auto my_bind_data = new MyCopyFunctionBindData();
	my_bind_data->max_size = max_size;
	my_bind_data->min_size = min_size;

	duckdb_copy_function_bind_set_bind_data(info, my_bind_data, [](void *bind_data) {
		auto my_bind_data = (MyCopyFunctionBindData *)bind_data;
		delete my_bind_data;
	});

	duckdb_destroy_value(&options);
	duckdb_destroy_logical_type(&column_type);
}

static void MyCopyFunctionInit(duckdb_copy_function_global_init_info info) {
	// COVERAGE
	duckdb_copy_function_global_init_set_error(nullptr, "foo");
	duckdb_copy_function_global_init_set_error(info, nullptr);
	duckdb_copy_function_global_init_set_error(nullptr, nullptr);
	REQUIRE(duckdb_copy_function_global_init_get_client_context(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_global_init_get_extra_info(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_global_init_get_bind_data(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_global_init_get_file_path(nullptr) == nullptr);
	duckdb_copy_function_global_init_set_global_state(nullptr, nullptr, nullptr);

	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_global_init_get_bind_data(info);
	auto extra_info = (MyCopyFunctionExtraInfo *)duckdb_copy_function_global_init_get_extra_info(info);
	auto client_context = duckdb_copy_function_global_init_get_client_context(info);

	if (bind_data->min_size == extra_info->illegal_min_value) {
		// Ooops, forgot to check this in the bind!
		duckdb_copy_function_global_init_set_error(info, "My bad, min_size cannot be set to that value!");
		duckdb_destroy_client_context(&client_context);
		return;
	}

	// Initialize state
	auto g_state = new MyCopyFunctionGlobalState();
	duckdb_copy_function_global_init_set_global_state(info, g_state, [](void *state) {
		auto g_state = (MyCopyFunctionGlobalState *)state;
		delete g_state;
	});

	// Setup file system and open the file
	g_state->total_written_bytes = 0;
	g_state->file_system = duckdb_client_context_get_file_system(client_context);

	auto file_path = duckdb_copy_function_global_init_get_file_path(info);
	auto file_flag = duckdb_create_file_open_options();
	duckdb_file_open_options_set_flag(file_flag, DUCKDB_FILE_FLAG_WRITE, true);
	duckdb_file_open_options_set_flag(file_flag, DUCKDB_FILE_FLAG_CREATE, true);

	if (duckdb_file_system_open(g_state->file_system, file_path, file_flag, &g_state->file_handle) != DuckDBSuccess) {
		auto error_data = duckdb_file_system_error_data(g_state->file_system);
		duckdb_copy_function_global_init_set_error(info, duckdb_error_data_message(error_data));
		duckdb_destroy_error_data(&error_data);
	}

	duckdb_destroy_file_open_options(&file_flag);
	duckdb_destroy_client_context(&client_context);
}

static void MyCopyFunctionSink(duckdb_copy_function_sink_info info, duckdb_data_chunk input) {
	// COVERAGE
	duckdb_copy_function_sink_set_error(nullptr, "foo");
	duckdb_copy_function_sink_set_error(info, nullptr);
	duckdb_copy_function_sink_set_error(nullptr, nullptr);
	REQUIRE(duckdb_copy_function_sink_get_client_context(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_sink_get_extra_info(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_sink_get_bind_data(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_sink_get_global_state(nullptr) == nullptr);

	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_sink_get_bind_data(info);
	auto g_state = (MyCopyFunctionGlobalState *)duckdb_copy_function_sink_get_global_state(info);

	// Sink the data
	auto row_count = duckdb_data_chunk_get_size(input);
	auto col_vec = duckdb_data_chunk_get_vector(input, 0);
	auto col_data = (int64_t *)duckdb_vector_get_data(col_vec);

	for (idx_t r = 0; r < row_count; r++) {
		auto written = duckdb_file_handle_write(g_state->file_handle, &col_data[r], sizeof(int64_t));
		if (written != sizeof(int64_t)) {
			auto error_data = duckdb_file_handle_error_data(g_state->file_handle);
			duckdb_copy_function_sink_set_error(info, duckdb_error_data_message(error_data));
			duckdb_destroy_error_data(&error_data);
			return;
		}
		g_state->total_written_bytes += written;

		if (g_state->total_written_bytes > bind_data->max_size) {
			duckdb_copy_function_sink_set_error(info, "Wrote too much data");
			return;
		}
	}
}

static void MyCopyFunctionFinalize(duckdb_copy_function_finalize_info info) {
	// COVERAGE
	duckdb_copy_function_finalize_set_error(nullptr, "foo");
	duckdb_copy_function_finalize_set_error(info, nullptr);
	duckdb_copy_function_finalize_set_error(nullptr, nullptr);
	REQUIRE(duckdb_copy_function_finalize_get_client_context(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_finalize_get_extra_info(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_finalize_get_bind_data(nullptr) == nullptr);
	REQUIRE(duckdb_copy_function_finalize_get_global_state(nullptr) == nullptr);

	auto bind_data = (MyCopyFunctionBindData *)duckdb_copy_function_finalize_get_bind_data(info);
	auto g_state = (MyCopyFunctionGlobalState *)duckdb_copy_function_finalize_get_global_state(info);

	// Check that we actually wrote enough!
	if (g_state->total_written_bytes < bind_data->min_size) {
		duckdb_copy_function_finalize_set_error(info, "Wrote too little data");
		return;
	}
}

//----------------------------------------------------------------------------------------------------------------------
// COPY (...) FROM (...)
//----------------------------------------------------------------------------------------------------------------------
struct MyCopyFromFunctionBindData {
	string file_path;
	int32_t max_size = 0;
	int32_t min_size = 0;
	duckdb_client_context client_context = nullptr;

	~MyCopyFromFunctionBindData() {
		if (client_context) {
			duckdb_destroy_client_context(&client_context);
		}
	}
};

struct MyCopyFromFunctionState {
	duckdb_file_system file_system = nullptr;
	duckdb_file_handle file_handle = nullptr;
	int64_t total_read_bytes = 0;

	~MyCopyFromFunctionState() {
		if (file_handle) {
			duckdb_destroy_file_handle(&file_handle);
		}
		if (file_system) {
			duckdb_destroy_file_system(&file_system);
		}
	}
};

static void MyCopyFromFunctionBind(duckdb_bind_info info) {
	// COVERAGE
	REQUIRE(duckdb_table_function_bind_get_result_column_count(nullptr) == 0);
	REQUIRE(duckdb_table_function_bind_get_result_column_name(nullptr, 0) == nullptr);
	REQUIRE(duckdb_table_function_bind_get_result_column_name(info, 9999) == nullptr);
	REQUIRE(duckdb_table_function_bind_get_result_column_type(nullptr, 0) == nullptr);
	REQUIRE(duckdb_table_function_bind_get_result_column_type(info, 9999) == nullptr);

	// Ensure we have exactly one expected column of type BIGINT
	auto result_column_count = duckdb_table_function_bind_get_result_column_count(info);
	if (result_column_count != 1) {
		auto error_msg = "MY_COPY: Expected exactly one target column!";
		duckdb_bind_set_error(info, error_msg);
		return;
	}

	for (idx_t i = 0; i < result_column_count; i++) {
		auto col_type = duckdb_table_function_bind_get_result_column_type(info, i);
		if (duckdb_get_type_id(col_type) != DUCKDB_TYPE_BIGINT) {
			auto col_name = duckdb_table_function_bind_get_result_column_name(info, i);
			auto error_msg = StringUtil::Format("MY_COPY: Target column '%s' is not of type BIGINT!", col_name);
			duckdb_bind_set_error(info, error_msg.c_str());
			duckdb_destroy_logical_type(&col_type);
			return;
		}
		duckdb_destroy_logical_type(&col_type);
	}

	auto file_path = duckdb_bind_get_parameter(info, 0);
	auto max_size = duckdb_bind_get_named_parameter(info, "MAX_SIZE");
	auto min_size = duckdb_bind_get_named_parameter(info, "MIN_SIZE");

	if (!file_path || !max_size || !min_size) {
		duckdb_destroy_value(&file_path);
		duckdb_destroy_value(&max_size);
		duckdb_destroy_value(&min_size);
		duckdb_bind_set_error(info, "MY_COPY: Error retrieving parameters!");
		return;
	}

	auto bind_data = new MyCopyFromFunctionBindData();

	// Get and set parameters
	auto file_path_str = duckdb_get_varchar(file_path);
	bind_data->file_path = string(file_path_str);
	duckdb_free(file_path_str);

	bind_data->max_size = duckdb_get_int32(max_size);
	bind_data->min_size = duckdb_get_int32(min_size);

	duckdb_destroy_value(&file_path);
	duckdb_destroy_value(&max_size);
	duckdb_destroy_value(&min_size);

	// Also get file client context
	duckdb_table_function_get_client_context(info, &bind_data->client_context);

	duckdb_bind_set_bind_data(info, bind_data, [](void *bind_data) {
		auto my_bind_data = (MyCopyFromFunctionBindData *)bind_data;
		delete my_bind_data;
	});
}

static void MyCopyFromFunctionInit(duckdb_init_info info) {
	auto bind_data = (MyCopyFromFunctionBindData *)duckdb_init_get_bind_data(info);
	auto state = new MyCopyFromFunctionState();

	auto client_context = bind_data->client_context;
	state->file_system = duckdb_client_context_get_file_system(client_context);
	auto file_flag = duckdb_create_file_open_options();
	duckdb_file_open_options_set_flag(file_flag, DUCKDB_FILE_FLAG_READ, true);

	auto ok = duckdb_file_system_open(state->file_system, bind_data->file_path.c_str(), file_flag, &state->file_handle);
	if (ok == DuckDBError) {
		auto error_data = duckdb_file_system_error_data(state->file_system);
		duckdb_init_set_error(info, duckdb_error_data_message(error_data));
		duckdb_destroy_error_data(&error_data);
		duckdb_destroy_file_open_options(&file_flag);
		delete state;
		return;
	}

	duckdb_destroy_file_open_options(&file_flag);
	state->total_read_bytes = 0;

	duckdb_init_set_init_data(info, state, [](void *state) {
		auto my_state = (MyCopyFromFunctionState *)state;
		delete my_state;
	});
}

static void MyCopyFromFunction(duckdb_function_info info, duckdb_data_chunk output) {
	// Now read data from file
	auto bind_data = (MyCopyFromFunctionBindData *)duckdb_function_get_bind_data(info);
	auto state = (MyCopyFromFunctionState *)duckdb_function_get_init_data(info);

	auto col_vec = duckdb_data_chunk_get_vector(output, 0);
	auto col_data = (int64_t *)duckdb_vector_get_data(col_vec);

	idx_t read_count = 0;
	for (read_count = 0; read_count < STANDARD_VECTOR_SIZE; read_count++) {
		int64_t read_value;
		auto read_bytes = duckdb_file_handle_read(state->file_handle, &read_value, sizeof(int64_t));
		if (read_bytes == 0) {
			// EOF
			if (state->total_read_bytes < bind_data->min_size) {
				duckdb_function_set_error(info, "Read too little data");
				return;
			}
			break;
		}

		if (read_bytes < 0) {
			auto error_data = duckdb_file_handle_error_data(state->file_handle);
			duckdb_function_set_error(info, duckdb_error_data_message(error_data));
			duckdb_destroy_error_data(&error_data);
			return;
		}

		if (read_bytes != sizeof(int64_t)) {
			duckdb_function_set_error(info, "Could not read full value");
			return;
		}

		col_data[read_count] = read_value;
		state->total_read_bytes += read_bytes;

		if (state->total_read_bytes > bind_data->max_size) {
			duckdb_function_set_error(info, "Read too much data");
			return;
		}
	}

	// Set the output count
	duckdb_data_chunk_set_size(output, read_count);
}

//----------------------------------------------------------------------------------------------------------------------
// Register
//----------------------------------------------------------------------------------------------------------------------

TEST_CASE("Test Copy Functions in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_copy_function func = duckdb_create_copy_function();
	REQUIRE(func != nullptr);

	// Set my extra info
	auto my_extra_info = new MyCopyFunctionExtraInfo();
	my_extra_info->illegal_min_value = 42;
	duckdb_copy_function_set_extra_info(func, my_extra_info, [](void *data) {
		auto my_extra_info = (MyCopyFunctionExtraInfo *)data;
		delete my_extra_info;
	});

	// Need to have a name
	auto status = duckdb_register_copy_function(tester.connection, func);
	REQUIRE(status == DuckDBError);

	// Set name
	duckdb_copy_function_set_name(func, "my_copy");

	status = duckdb_register_copy_function(tester.connection, func);
	REQUIRE(status == DuckDBError);

	// Need to have function pointers set
	duckdb_copy_function_set_bind(func, MyCopyFunctionBind);
	duckdb_copy_function_set_global_init(func, MyCopyFunctionInit);
	duckdb_copy_function_set_sink(func, MyCopyFunctionSink);
	duckdb_copy_function_set_finalize(func, MyCopyFunctionFinalize);

	// Also add a scan function
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

	auto scan_func = duckdb_create_table_function();
	duckdb_table_function_add_parameter(scan_func, varchar_type);
	duckdb_table_function_add_named_parameter(scan_func, "MAX_SIZE", int_type);
	duckdb_table_function_add_named_parameter(scan_func, "MIN_SIZE", int_type);
	duckdb_table_function_set_bind(scan_func, MyCopyFromFunctionBind);
	duckdb_table_function_set_init(scan_func, MyCopyFromFunctionInit);
	duckdb_table_function_set_function(scan_func, MyCopyFromFunction);
	duckdb_table_function_set_name(scan_func, "my_copy");

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&int_type);

	duckdb_copy_function_set_copy_from_function(func, scan_func);

	duckdb_destroy_table_function(&scan_func);

	status = duckdb_register_copy_function(tester.connection, func);
	REQUIRE(status == DuckDBSuccess);

	auto file_path = TestDirectoryPath() + "/" + "test_copy";

	// Try write too little
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s1.txt' (FORMAT MY_COPY, MIN_SIZE 2000, MAX_SIZE 1000)",
	    file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Wrote too little data"));

	// Try write too much
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s2.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 5)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Wrote too much data"));

	// Try write some non-int data
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i::VARCHAR FROM range(10) as r(i)) TO '%s3.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 100)",
	    file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Expected column of type BIGINT"));

	// Try write some data with illegal min-size
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(3) as r(i)) TO '%s4.txt' (FORMAT MY_COPY, MIN_SIZE 42, MAX_SIZE 100)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "My bad, min_size cannot be set to that value!"));

	// Try write with unknown option
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s5.txt' (FORMAT MY_COPY, MIN_SIZE 0, UNKNOWN 5)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Unknown option given"));

	// Try write with too many options
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s6.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 100, EXTRA 5)",
	    file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Too many options given"));

	// Try write with a non-int option
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s7.txt' (FORMAT MY_COPY, MIN_SIZE 'hello', MAX_SIZE 100)",
	    file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Options must be of type INT"));

	// Try write just right
	result = tester.Query(StringUtil::Format(
	    "COPY (SELECT i FROM range(10) as r(i)) TO '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 100)", file_path));
	REQUIRE_NO_FAIL(*result);

	//----------------------------------
	// Now read with COPY ... FROM (...)
	//----------------------------------

	// Now try to read it back in
	result = tester.Query("CREATE TABLE my_table(i BIGINT)");
	REQUIRE_NO_FAIL(*result);

	// Read non-existing file
	result = tester.Query("COPY my_table FROM 'non_existing_file.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 100)");
	REQUIRE_FAIL(result);

	// Read with too small max size
	result = tester.Query(
	    StringUtil::Format("COPY my_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 10)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Read too much data"));

	// Read with too large min size
	result = tester.Query(
	    StringUtil::Format("COPY my_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 1000, MAX_SIZE 10000)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Read too little data"));

	// Read with non-int target column
	result = tester.Query("CREATE TABLE my_varchar_table(i VARCHAR)");
	REQUIRE_NO_FAIL(*result);
	result = tester.Query(StringUtil::Format(
	    "COPY my_varchar_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 10000)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Target column 'i' is not of type BIGINT!"));
	result = tester.Query("DROP TABLE my_varchar_table");
	REQUIRE_NO_FAIL(*result);

	// Read with two target columns
	result = tester.Query("CREATE TABLE my_two_col_table(i BIGINT, j BIGINT)");
	REQUIRE_NO_FAIL(*result);
	result = tester.Query(StringUtil::Format(
	    "COPY my_two_col_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 10000)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "Expected exactly one target column!"));
	result = tester.Query("DROP TABLE my_two_col_table");
	REQUIRE_NO_FAIL(*result);

	// Read with unknown option
	result = tester.Query(StringUtil::Format(
	    "COPY my_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 10000, UNKNOWN 5)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(),
	                             "'UNKNOWN' is not a supported option for copy function 'my_copy'"));

	// Read with missing option
	result =
	    tester.Query(StringUtil::Format("COPY my_table FROM '%s8.txt' (FORMAT MY_COPY, MAX_SIZE 10000)", file_path));
	REQUIRE_FAIL(result);
	REQUIRE(StringUtil::Contains(result->ErrorMessage(), "MY_COPY: Error retrieving parameters!"));

	// Read just right
	result = tester.Query(
	    StringUtil::Format("COPY my_table FROM '%s8.txt' (FORMAT MY_COPY, MIN_SIZE 0, MAX_SIZE 10000)", file_path));
	REQUIRE_NO_FAIL(*result);
	result = tester.Query("SELECT COUNT(*) FROM my_table");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 10);
	result = tester.Query("SELECT SUM(i) FROM my_table");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 45);

	// Destroy
	duckdb_destroy_copy_function(&func);
	duckdb_destroy_copy_function(&func);
}
