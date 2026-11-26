#include "test_capi_v2.hpp"

#include <atomic>
#include <string>

// ---------------------------------------------------------------------------
// V2 data source tests: a data source holds the callbacks that scan a source of
// rows, and a table function takes it as its scan. The same data source scans
// for several functions, each with the bind data of its own call.
//
// Callbacks avoid Catch assertions: a REQUIRE would throw through the C
// callback boundary into the engine. On failure they populate the provided
// error slot and return.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

// Run a query producing a single BIGINT cell.
int64_t DsQueryI64(duckdb_v2_connection_handle conn, std::string_view sql) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, sql, &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr);
	duckdb_v2_vector_view view {};
	duckdb_v2_vector_get_view(vec, &view, nullptr);
	auto out = static_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return out;
}

duckdb_v2_data_source_handle DsMakeSource(duckdb_v2_factory_handle factory) {
	duckdb_v2_data_source_handle source = nullptr;
	REQUIRE(duckdb_v2_data_source_create(factory, &source, nullptr) == DUCKDB_V2_ERROR_NONE);
	return source;
}

duckdb_v2_table_function_handle DsMakeFunction(duckdb_v2_factory_handle factory, std::string_view name) {
	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create(factory, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_name(function, name.data(), name.size(), nullptr) == DUCKDB_V2_ERROR_NONE);
	return function;
}

// ---------------------------------------------------------------------------
// The bind side: a function declaring BIGINT columns and, as bind data, the number of rows to produce, which the
// data source reads. The number of columns, and how the argument is scaled, tell the functions apart.
// ---------------------------------------------------------------------------

struct DsCountBind {
	int64_t count = 0;
	// What the function's own user data was when it bound
	int64_t function_user_data = 0;
};

void DsDeleteCountBind(void *ptr) {
	delete static_cast<DsCountBind *>(ptr);
}

template <int64_t SCALE, int COLUMNS>
void DsCountBindCb(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
                   duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	duckdb_v2_value_handle value = nullptr;
	if (duckdb_v2_function_bind_get_arg_value(info, 0, &value, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	int64_t count = 0;
	auto rc = duckdb_v2_value_get_bigint(value, &count, err);
	duckdb_v2_value_destroy(&value);
	if (rc != DUCKDB_V2_ERROR_NONE) {
		return;
	}

	duckdb_v2_factory_handle factory = nullptr;
	duckdb_v2_context_get_factory(context, &factory, nullptr);
	for (int i = 0; i < COLUMNS; i++) {
		duckdb_v2_logical_type_handle bigint = nullptr;
		if (duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, nullptr,
		                                          0, &bigint, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		std::string name = std::string(1, static_cast<char>('a' + i));
		rc = duckdb_v2_table_function_bind_add_result_column(result, name.data(), name.size(), bigint, err);
		duckdb_v2_logical_type_destroy(&bigint);
		if (rc != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}

	void *user_data = nullptr;
	if (duckdb_v2_function_bind_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto bind = new DsCountBind {count * SCALE, user_data ? *static_cast<int64_t *>(user_data) : 0};
	duckdb_v2_opaque bind_data = {bind, DsDeleteCountBind, nullptr};
	duckdb_v2_function_bind_set_bind_data(info, &bind_data, err);
}

// ---------------------------------------------------------------------------
// The scan side: it produces `count` rows. Column c of row r holds r + c * 1000, unless a user data of the source says
// otherwise: then every value is that number, to see which user data reached the scan.
// ---------------------------------------------------------------------------

struct DsCountGlobal {
	int64_t position = 0;
	// The user data seen when the scan was set up
	int64_t user_data_seen = -1;
};

void DsDeleteCountGlobal(void *ptr) {
	delete static_cast<DsCountGlobal *>(ptr);
}

void DsCountInitGlobalCb(duckdb_v2_data_source_init_global_info_handle info, duckdb_v2_context_handle,
                         duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	if (duckdb_v2_data_source_init_global_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto state = new DsCountGlobal();
	state->user_data_seen = user_data ? *static_cast<int64_t *>(user_data) : -1;
	duckdb_v2_opaque global = {state, DsDeleteCountGlobal, nullptr};
	duckdb_v2_data_source_init_global_set_global_state(info, &global, err);
}

// Produces the columns the chunk holds. With projection pushdown the vectors stand for the requested columns, whose
// declared index the callback looks up; without it they are all the declared columns.
void DsCountExecCb(duckdb_v2_data_source_exec_info_handle info, duckdb_v2_context_handle,
                   duckdb_v2_error_info_handle *err) {
	void *bind_ptr = nullptr;
	void *global_ptr = nullptr;
	duckdb_v2_data_chunk_handle chunk = nullptr;
	idx_t column_count = 0;
	if (duckdb_v2_data_source_exec_get_bind_data(info, &bind_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_source_exec_get_global_state(info, &global_ptr, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_source_exec_get_output_chunk(info, &chunk, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_data_source_exec_get_column_count(info, &column_count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	auto &bind = *static_cast<DsCountBind *>(bind_ptr);
	auto &global = *static_cast<DsCountGlobal *>(global_ptr);

	auto remaining = bind.count - global.position;
	auto produced =
	    remaining < static_cast<int64_t>(STANDARD_VECTOR_SIZE) ? remaining : static_cast<int64_t>(STANDARD_VECTOR_SIZE);
	if (produced < 0) {
		produced = 0;
	}
	for (idx_t c = 0; c < column_count; c++) {
		idx_t declared = 0;
		duckdb_v2_vector_handle vec = nullptr;
		void *raw = nullptr;
		if (duckdb_v2_data_source_exec_get_column_index(info, c, &declared, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(chunk, c, &vec, err) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_get_data_mutable(vec, &raw, err) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		auto *out = static_cast<int64_t *>(raw);
		for (int64_t i = 0; i < produced; i++) {
			out[i] = global.user_data_seen >= 0 ? global.user_data_seen
			                                    : global.position + i + static_cast<int64_t>(declared) * 1000;
		}
		if (c == 0) {
			duckdb_v2_vector_set_size(vec, static_cast<idx_t>(produced), err);
		}
	}
	global.position += produced;
}

// Where the callbacks of a scan observed something that the test then asserts on
struct DsProbe {
	std::atomic<int> exec_calls {0};
	std::atomic<int> max_column_count {0};
};
DsProbe ds_probe;

void DsProbingExecCb(duckdb_v2_data_source_exec_info_handle info, duckdb_v2_context_handle context,
                     duckdb_v2_error_info_handle *err) {
	idx_t column_count = 0;
	if (duckdb_v2_data_source_exec_get_column_count(info, &column_count, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	ds_probe.exec_calls++;
	auto seen = static_cast<int>(column_count);
	auto previous = ds_probe.max_column_count.load();
	while (seen > previous && !ds_probe.max_column_count.compare_exchange_weak(previous, seen)) {
	}
	DsCountExecCb(info, context, err);
}

duckdb_v2_data_source_handle DsMakeCountSource(duckdb_v2_factory_handle factory) {
	auto source = DsMakeSource(factory);
	REQUIRE(duckdb_v2_data_source_set_init_global_callback(source, DsCountInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_source_set_exec_callback(source, DsCountExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	return source;
}

void DsSetCountSignature(duckdb_v2_factory_handle factory, duckdb_v2_table_function_handle function) {
	auto bigint = MakeType(factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_table_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	std::string_view name = "n";
	REQUIRE(duckdb_v2_function_signature_add_parameter(sig, name.data(), name.size(), bigint, nullptr,
	                                                   DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
	                                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_logical_type_destroy(&bigint);
}

// Registers a function `name(n)` that binds with `bind_cb` and scans with `source`
void DsRegisterWithSource(EnvFixture &fx, std::string_view name, duckdb_v2_table_function_bind_callback_fn bind_cb,
                          duckdb_v2_data_source_handle source, int64_t *user_data = nullptr,
                          bool projection_pushdown = false) {
	auto function = DsMakeFunction(fx.factory, name);
	DsSetCountSignature(fx.factory, function);
	REQUIRE(duckdb_v2_table_function_set_projection_pushdown(function, projection_pushdown, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, bind_cb, nullptr) == DUCKDB_V2_ERROR_NONE);
	if (user_data) {
		duckdb_v2_opaque data = {user_data, nullptr, nullptr};
		REQUIRE(duckdb_v2_table_function_set_user_data(function, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	}
	REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
}

} // namespace

TEST_CASE("V2 data source: one data source scans for several table functions", "[capi_v2][data_source]") {
	EnvFixture fx;

	auto source = DsMakeCountSource(fx.factory);
	DsRegisterWithSource(fx, "ds_one", DsCountBindCb<1, 1>, source);
	DsRegisterWithSource(fx, "ds_double", DsCountBindCb<2, 1>, source);
	DsRegisterWithSource(fx, "ds_wide", DsCountBindCb<1, 3>, source);
	// The functions keep what they took: the data source can go
	REQUIRE(duckdb_v2_data_source_destroy(&source) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(source == nullptr);

	// Each function scans with the bind data of its own call
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_one(10)") == 10);
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_double(10)") == 20);
	REQUIRE(DsQueryI64(fx.conn, "SELECT sum(a)::BIGINT FROM ds_one(10)") == 45);
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_one(5000)") == 5000);
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_one(0)") == 0);

	// The columns the function declares are the ones the data source fills
	REQUIRE(DsQueryI64(fx.conn, "SELECT sum(a + b + c)::BIGINT FROM ds_wide(4)") == 3 * 6 + 4 * 3000);
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM ds_wide(3)", &result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ColumnCount(result) == 3);
	REQUIRE(DrainRowCount(result) == 3);
	duckdb_v2_result_destroy(&result);
}

TEST_CASE("V2 data source: user data is the source's own, else the function's", "[capi_v2][data_source]") {
	EnvFixture fx;
	int64_t function_payload = 42;
	int64_t source_payload = 7;

	// A data source with user data of its own: its callbacks see that one, the bind callback the function's
	auto with_data = DsMakeCountSource(fx.factory);
	duckdb_v2_opaque source_data = {&source_payload, nullptr, nullptr};
	REQUIRE(duckdb_v2_data_source_set_user_data(with_data, &source_data, nullptr) == DUCKDB_V2_ERROR_NONE);
	DsRegisterWithSource(fx, "ds_own_data", DsCountBindCb<1, 1>, with_data, &function_payload);
	duckdb_v2_data_source_destroy(&with_data);
	REQUIRE(DsQueryI64(fx.conn, "SELECT min(a)::BIGINT FROM ds_own_data(3)") == 7);
	REQUIRE(DsQueryI64(fx.conn, "SELECT max(a)::BIGINT FROM ds_own_data(3)") == 7);

	// Without, they see the user data of the function
	auto without_data = DsMakeCountSource(fx.factory);
	DsRegisterWithSource(fx, "ds_function_data", DsCountBindCb<1, 1>, without_data, &function_payload);
	duckdb_v2_data_source_destroy(&without_data);
	REQUIRE(DsQueryI64(fx.conn, "SELECT max(a)::BIGINT FROM ds_function_data(3)") == 42);

	// User data with a null pointer counts as none: they see the user data of the function
	auto null_data = DsMakeCountSource(fx.factory);
	duckdb_v2_opaque empty = {};
	REQUIRE(duckdb_v2_data_source_set_user_data(null_data, &empty, nullptr) == DUCKDB_V2_ERROR_NONE);
	DsRegisterWithSource(fx, "ds_null_data", DsCountBindCb<1, 1>, null_data, &function_payload);
	duckdb_v2_data_source_destroy(&null_data);
	REQUIRE(DsQueryI64(fx.conn, "SELECT max(a)::BIGINT FROM ds_null_data(3)") == 42);

	// And without any, they see none
	auto neither = DsMakeCountSource(fx.factory);
	DsRegisterWithSource(fx, "ds_no_data", DsCountBindCb<1, 1>, neither);
	duckdb_v2_data_source_destroy(&neither);
	REQUIRE(DsQueryI64(fx.conn, "SELECT max(a)::BIGINT FROM ds_no_data(3)") == 2);
}

TEST_CASE("V2 data source: a function keeps a copy of the data source it took", "[capi_v2][data_source]") {
	EnvFixture fx;

	auto source = DsMakeCountSource(fx.factory);
	auto function = DsMakeFunction(fx.factory, "ds_copy");
	DsSetCountSignature(fx.factory, function);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);

	// Changing the data source afterwards does not change the function
	REQUIRE(duckdb_v2_data_source_set_exec_callback(source, DsProbingExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	ds_probe.exec_calls = 0;
	REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_copy(10)") == 10);
	REQUIRE(ds_probe.exec_calls == 0);
	duckdb_v2_table_function_destroy(&function);

	// A function taking it afterwards does see the change
	DsRegisterWithSource(fx, "ds_copy_changed", DsCountBindCb<1, 1>, source);
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_copy_changed(10)") == 10);
	REQUIRE(ds_probe.exec_calls > 0);
	duckdb_v2_data_source_destroy(&source);
}

TEST_CASE("V2 data source: a table function without a data source cannot be registered", "[capi_v2][data_source]") {
	EnvFixture fx;

	auto function = DsMakeFunction(fx.factory, "ds_missing");
	DsSetCountSignature(fx.factory, function);
	REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	// Taking a later data source replaces an earlier one
	auto probing = DsMakeSource(fx.factory);
	REQUIRE(duckdb_v2_data_source_set_exec_callback(probing, DsProbingExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto source = DsMakeCountSource(fx.factory);
	REQUIRE(duckdb_v2_table_function_set_data_source(function, probing, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_data_source_destroy(&probing);
	duckdb_v2_data_source_destroy(&source);
	REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
	ds_probe.exec_calls = 0;
	REQUIRE(DsQueryI64(fx.conn, "SELECT count(*) FROM ds_missing(10)") == 10);
	REQUIRE(ds_probe.exec_calls == 0);
}

TEST_CASE("V2 data source: projection pushdown reaches the scan", "[capi_v2][data_source]") {
	EnvFixture fx;

	auto source = DsMakeSource(fx.factory);
	REQUIRE(duckdb_v2_data_source_set_init_global_callback(source, DsCountInitGlobalCb, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_source_set_exec_callback(source, DsProbingExecCb, nullptr) == DUCKDB_V2_ERROR_NONE);
	DsRegisterWithSource(fx, "ds_projected", DsCountBindCb<1, 3>, source, nullptr, true);
	duckdb_v2_data_source_destroy(&source);

	// Only the column the query reads is produced, and it is the declared column c
	ds_probe.exec_calls = 0;
	ds_probe.max_column_count = 0;
	REQUIRE(DsQueryI64(fx.conn, "SELECT sum(c)::BIGINT FROM ds_projected(4)") == 6 + 4 * 2000);
	REQUIRE(ds_probe.exec_calls > 0);
	REQUIRE(ds_probe.max_column_count == 1);
}

TEST_CASE("V2 data source: refusals", "[capi_v2][data_source]") {
	EnvFixture fx;

	// A data source without an exec callback cannot scan
	{
		auto source = DsMakeSource(fx.factory);
		auto function = DsMakeFunction(fx.factory, "ds_no_exec");
		DsSetCountSignature(fx.factory, function);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
		duckdb_v2_data_source_destroy(&source);
	}

	// A function cannot declare a partitioning without a data source with the partition data that backs it
	{
		auto source = DsMakeCountSource(fx.factory);
		auto function = DsMakeFunction(fx.factory, "ds_no_partition_data");
		DsSetCountSignature(fx.factory, function);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_partitioning_callback(
		            function,
		            [](duckdb_v2_table_function_partitioning_info_handle, duckdb_v2_context_handle,
		               duckdb_v2_error_info_handle *) {},
		            nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
		duckdb_v2_data_source_destroy(&source);
	}

	// A virtual column needs projection pushdown to be asked for
	{
		auto source = DsMakeCountSource(fx.factory);
		auto function = DsMakeFunction(fx.factory, "ds_virtual_column");
		DsSetCountSignature(fx.factory, function);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_add_virtual_column(function, DUCKDB_V2_TABLE_VIRTUAL_COLUMN_FILE_ROW_NUMBER,
		                                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		REQUIRE(duckdb_v2_table_function_set_projection_pushdown(function, true, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_table_function_destroy(&function);
		duckdb_v2_data_source_destroy(&source);
	}

	// A refused registration leaves nothing behind: the retry scans with the user data set before it
	{
		int64_t first = 42;
		int64_t second = 99;
		auto source = DsMakeCountSource(fx.factory);
		auto function = DsMakeFunction(fx.factory, "ds_retry");
		DsSetCountSignature(fx.factory, function);
		REQUIRE(duckdb_v2_table_function_set_bind_callback(function, DsCountBindCb<1, 1>, nullptr) ==
		        DUCKDB_V2_ERROR_NONE);
		duckdb_v2_opaque first_data = {&first, nullptr, nullptr};
		REQUIRE(duckdb_v2_table_function_set_user_data(function, &first_data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_add_virtual_column(function, DUCKDB_V2_TABLE_VIRTUAL_COLUMN_FILE_ROW_NUMBER,
		                                                    nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) ==
		        DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_opaque second_data = {&second, nullptr, nullptr};
		REQUIRE(duckdb_v2_table_function_set_user_data(function, &second_data, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_table_function_set_projection_pushdown(function, true, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(duckdb_v2_connection_register_table_function(fx.conn, function, nullptr) == DUCKDB_V2_ERROR_NONE);
		duckdb_v2_table_function_destroy(&function);
		duckdb_v2_data_source_destroy(&source);
		REQUIRE(DsQueryI64(fx.conn, "SELECT max(a)::BIGINT FROM ds_retry(3)") == 99);
	}

	// A data source of another database cannot scan for this one's functions
	{
		EnvFixture other;
		auto source = DsMakeCountSource(other.factory);
		auto function = DsMakeFunction(fx.factory, "ds_other_database");
		REQUIRE(duckdb_v2_table_function_set_data_source(function, source, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
		duckdb_v2_table_function_destroy(&function);
		duckdb_v2_data_source_destroy(&source);
	}
}

TEST_CASE("V2 data source: null arguments and destroy null-safety", "[capi_v2][data_source]") {
	EnvFixture fx;
	duckdb_v2_data_source_handle source = nullptr;

	REQUIRE(duckdb_v2_data_source_create(nullptr, &source, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_data_source_create(fx.factory, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(source == nullptr);

	REQUIRE(duckdb_v2_data_source_set_exec_callback(nullptr, DsCountExecCb, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_data_source_set_user_data(nullptr, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);

	auto function = DsMakeFunction(fx.factory, "ds_null_args");
	REQUIRE(duckdb_v2_table_function_set_data_source(nullptr, source, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(duckdb_v2_table_function_set_data_source(function, nullptr, nullptr) == DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_table_function_destroy(&function);

	// Destroying nothing, and destroying twice, is fine
	REQUIRE(duckdb_v2_data_source_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_source_destroy(&source) == DUCKDB_V2_ERROR_NONE);
	source = DsMakeSource(fx.factory);
	REQUIRE(duckdb_v2_data_source_destroy(&source) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_data_source_destroy(&source) == DUCKDB_V2_ERROR_NONE);
}

} // namespace test_capi_v2
