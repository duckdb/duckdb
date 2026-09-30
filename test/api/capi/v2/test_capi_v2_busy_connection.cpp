#include "test_capi_v2.hpp"

#include <cstring>

// ---------------------------------------------------------------------------
// V2 busy-connection tests: connection-scoped registration is refused while
// the connection has a live result, and a refused handle is left intact so it
// can be registered once the result is gone.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

// Named, not anonymous: unity builds share one anonymous namespace across the test files.
namespace busy_connection {

duckdb_v2_identifier_t Ident(const char *s) {
	return duckdb_v2_identifier_t {s, std::strlen(s)};
}

int destroyed_count = 0;

void CountDestroy(void *data) {
	destroyed_count++;
	delete static_cast<int32_t *>(data);
}

// A caller-owned user data payload whose destruction is counted.
duckdb_v2_opaque CountedUserData(int32_t value) {
	destroyed_count = 0;
	return duckdb_v2_opaque {new int32_t(value), CountDestroy, nullptr};
}

// A live result: the query has not been drained, so the connection stays busy.
duckdb_v2_result_handle LiveResult(duckdb_v2_connection_handle conn) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, "SELECT 42", &result) == DUCKDB_V2_ERROR_NONE);
	return result;
}

// out[i] = *user_data; a missing user data fails the query.
void UserDataExec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle,
                  duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	duckdb_v2_vector_handle out = nullptr;
	idx_t count = 0;
	void *raw = nullptr;
	if (duckdb_v2_scalar_function_exec_get_user_data(info, &user_data, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_result(info, &out, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_row_count(info, &count, err) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_vector_get_data_mutable(out, &raw, err) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (!user_data) {
		duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_API);
		auto text_str = Convert("user data missing");
		duckdb_v2_error_info_set_text(*err, &text_str);
		return;
	}
	auto *out_data = static_cast<int32_t *>(raw);
	for (idx_t i = 0; i < count; i++) {
		out_data[i] = *static_cast<int32_t *>(user_data);
	}
}

void NoopAggregateSize(duckdb_v2_aggregate_function_size_info_handle, duckdb_v2_error_info_handle *) {
}
void NoopAggregateInit(duckdb_v2_aggregate_function_init_info_handle, duckdb_v2_error_info_handle *) {
}
void NoopAggregateUpdate(duckdb_v2_aggregate_function_update_info_handle, duckdb_v2_error_info_handle *) {
}
void NoopAggregateCombine(duckdb_v2_aggregate_function_combine_info_handle, duckdb_v2_error_info_handle *) {
}
void NoopAggregateFinalize(duckdb_v2_aggregate_function_finalize_info_handle, duckdb_v2_error_info_handle *) {
}
void NoopTableBind(duckdb_v2_table_function_bind_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}
void NoopTableExec(duckdb_v2_table_function_exec_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}
void NoopCopyBatch(duckdb_v2_copy_to_batch_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}
void NoopCopyFlush(duckdb_v2_copy_to_flush_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}
void NoopCast(duckdb_v2_cast_function_exec_info_handle, duckdb_v2_context_handle, duckdb_v2_error_info_handle *) {
}

TEST_CASE("V2 busy connection: a refused scalar function keeps its user data", "[capi_v2][busy_connection]") {
	EnvFixture fx;
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_scalar_function_handle function = nullptr;
	REQUIRE(duckdb_v2_scalar_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Ident("busy_user_data");
	REQUIRE(duckdb_v2_scalar_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_scalar_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_scalar_function_set_exec_callback(function, UserDataExec, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto user_data = CountedUserData(7);
	REQUIRE(duckdb_v2_scalar_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(destroyed_count == 0);
	duckdb_v2_result_destroy(&result);

	REQUIRE(duckdb_v2_scalar_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_scalar_function_destroy(&function);

	REQUIRE(Query(fx.conn, "SELECT busy_user_data()", &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(static_cast<const int32_t *>(view.data)[SelAt(view.sel, 0)] == 7);
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 busy connection: a refused aggregate function keeps its user data", "[capi_v2][busy_connection]") {
	EnvFixture fx;
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);

	duckdb_v2_aggregate_function_handle function = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Ident("busy_aggregate");
	REQUIRE(duckdb_v2_aggregate_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_function_signature_handle sig = nullptr;
	REQUIRE(duckdb_v2_aggregate_function_get_signature(function, &sig, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_function_signature_set_return_type(sig, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_aggregate_function_set_size_callback(function, NoopAggregateSize, nullptr);
	duckdb_v2_aggregate_function_set_init_callback(function, NoopAggregateInit, nullptr);
	duckdb_v2_aggregate_function_set_update_callback(function, NoopAggregateUpdate, nullptr);
	duckdb_v2_aggregate_function_set_combine_callback(function, NoopAggregateCombine, nullptr);
	duckdb_v2_aggregate_function_set_finalize_callback(function, NoopAggregateFinalize, nullptr);
	auto user_data = CountedUserData(1);
	REQUIRE(duckdb_v2_aggregate_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_aggregate_function_register(function, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(destroyed_count == 0);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_aggregate_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_aggregate_function_destroy(&function);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 busy connection: a refused table function keeps its user data", "[capi_v2][busy_connection]") {
	EnvFixture fx;

	duckdb_v2_table_function_handle function = nullptr;
	REQUIRE(duckdb_v2_table_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Ident("busy_table");
	REQUIRE(duckdb_v2_table_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_set_bind_callback(function, NoopTableBind, nullptr);
	duckdb_v2_table_function_set_exec_callback(function, NoopTableExec, nullptr);
	auto user_data = CountedUserData(1);
	REQUIRE(duckdb_v2_table_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(destroyed_count == 0);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_table_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_function_destroy(&function);
}

TEST_CASE("V2 busy connection: a refused copy function keeps its user data", "[capi_v2][busy_connection]") {
	EnvFixture fx;

	duckdb_v2_copy_function_handle function = nullptr;
	REQUIRE(duckdb_v2_copy_function_create_with_connection(fx.conn, &function, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Ident("busy_copy");
	REQUIRE(duckdb_v2_copy_function_set_name(function, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_to_set_batch_callback(function, NoopCopyBatch, nullptr);
	duckdb_v2_copy_to_set_flush_callback(function, NoopCopyFlush, nullptr);
	auto user_data = CountedUserData(1);
	REQUIRE(duckdb_v2_copy_function_set_user_data(function, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(destroyed_count == 0);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_copy_function_register(function, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_copy_function_destroy(&function);
}

TEST_CASE("V2 busy connection: cast functions and custom types are refused", "[capi_v2][busy_connection]") {
	EnvFixture fx;
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto varchar = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR);

	duckdb_v2_cast_function_handle cast = nullptr;
	REQUIRE(duckdb_v2_cast_function_create_with_connection(fx.conn, &cast, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_set_source_type(cast, varchar, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_set_target_type(cast, integer, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_cast_function_set_exec_callback(cast, NoopCast, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto user_data = CountedUserData(1);
	REQUIRE(duckdb_v2_cast_function_set_user_data(cast, &user_data, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_custom_type_handle type = nullptr;
	REQUIRE(duckdb_v2_custom_type_create_with_connection(fx.conn, &type, nullptr) == DUCKDB_V2_ERROR_NONE);
	auto name = Ident("busy_type");
	REQUIRE(duckdb_v2_custom_type_set_name(type, &name, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_custom_type_set_base_type(type, integer, nullptr) == DUCKDB_V2_ERROR_NONE);

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_cast_function_register(cast, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(duckdb_v2_custom_type_register(type, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(destroyed_count == 0);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_cast_function_register(cast, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_custom_type_register(type, nullptr) == DUCKDB_V2_ERROR_NONE);

	duckdb_v2_custom_type_destroy(&type);
	duckdb_v2_cast_function_destroy(&cast);
	duckdb_v2_logical_type_destroy(&varchar);
	duckdb_v2_logical_type_destroy(&integer);
}

TEST_CASE("V2 busy connection: describing a table is refused", "[capi_v2][busy_connection]") {
	EnvFixture fx;
	ExecSQL(fx.conn, "CREATE TABLE t (i INTEGER)");

	auto text = Convert("t");
	duckdb_v2_qname_handle qname = nullptr;
	REQUIRE(duckdb_v2_qname_parse(&text, &qname, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_table_description_handle desc = nullptr;

	auto result = LiveResult(fx.conn);
	REQUIRE(duckdb_v2_connection_describe_table(fx.conn, qname, &desc, nullptr) == DUCKDB_V2_ERROR_RESOURCE_IN_USE);
	REQUIRE(desc == nullptr);
	duckdb_v2_result_destroy(&result);
	REQUIRE(duckdb_v2_connection_describe_table(fx.conn, qname, &desc, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(desc != nullptr);

	duckdb_v2_table_description_destroy(&desc);
	duckdb_v2_qname_destroy(&qname);
}

TEST_CASE("V2 busy connection: a failed cast leaves the live transaction intact", "[capi_v2][busy_connection]") {
	EnvFixture fx;
	auto integer = MakeType(fx.conn, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
	auto text = MakeVarcharValue(fx.conn, "abc");

	ExecSQL(fx.conn, "BEGIN");
	ExecSQL(fx.conn, "CREATE TABLE t AS SELECT 1 AS i");
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(fx.conn, "SELECT * FROM range(100000)", &result) == DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_data_chunk_destroy(&chunk);

	// Both run inside the live query's transaction; their failures must not invalidate it.
	duckdb_v2_value_handle out = nullptr;
	REQUIRE(duckdb_v2_value_cast_with_connection(fx.conn, text, integer, &out, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(out == nullptr);
	duckdb_v2_logical_type_handle bad_type = nullptr;
	auto bad_text = Convert("NOT_A_TYPE");
	REQUIRE(duckdb_v2_connection_create_type_from_text(fx.conn, &bad_text, &bad_type, nullptr) != DUCKDB_V2_ERROR_NONE);

	REQUIRE(DrainRowCount(result) > 0);
	duckdb_v2_result_destroy(&result);
	ExecSQL(fx.conn, "COMMIT");
	REQUIRE(Query(fx.conn, "SELECT * FROM t", &result) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(DrainRowCount(result) == 1);
	duckdb_v2_result_destroy(&result);

	duckdb_v2_value_destroy(&text);
	duckdb_v2_logical_type_destroy(&integer);
}

} // namespace busy_connection

} // namespace test_capi_v2
