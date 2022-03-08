#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Logical Types C API", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_BIGINT);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&type);

	duckdb_destroy_logical_type(nullptr);
}

TEST_CASE("Test DataChunk C API", "[capi]") {
	CAPITester tester;
	unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));

	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	tester.Query("CREATE TABLE test(i BIGINT, j SMALLINT)");

	duckdb_logical_type types[2];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	types[1] = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);

	auto data_chunk = duckdb_create_data_chunk(types, 2);
	REQUIRE(data_chunk);

	REQUIRE(duckdb_data_chunk_get_column_count(data_chunk) == 2);

	auto first_type = duckdb_data_chunk_get_column_type(data_chunk, 0);
	REQUIRE(duckdb_get_type_id(first_type) == DUCKDB_TYPE_BIGINT);
	duckdb_destroy_logical_type(&first_type);

	auto second_type = duckdb_data_chunk_get_column_type(data_chunk, 1);
	REQUIRE(duckdb_get_type_id(second_type) == DUCKDB_TYPE_SMALLINT);
	duckdb_destroy_logical_type(&second_type);

	REQUIRE(duckdb_data_chunk_get_column_type(data_chunk, 999) == nullptr);
	REQUIRE(duckdb_data_chunk_get_column_type(nullptr, 0) == nullptr);

	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);
	REQUIRE(duckdb_data_chunk_get_size(nullptr) == 0);

	// use the appender to insert a value using the data chunk API

	duckdb_appender appender;
	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);

	// append standard primitive values
	auto col1_ptr = (int64_t *)duckdb_data_chunk_get_data(data_chunk, 0);
	*col1_ptr = 42;
	auto col2_ptr = (int16_t *)duckdb_data_chunk_get_data(data_chunk, 1);
	*col2_ptr = 84;

	REQUIRE(duckdb_data_chunk_get_data(nullptr, 0) == nullptr);
	REQUIRE(duckdb_data_chunk_get_data(data_chunk, 999) == nullptr);

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
	REQUIRE(duckdb_append_data_chunk(appender, nullptr) == DuckDBError);
	REQUIRE(duckdb_append_data_chunk(nullptr, data_chunk) == DuckDBError);

	// append nulls
	duckdb_data_chunk_reset(data_chunk);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	duckdb_data_chunk_ensure_validity_writable(data_chunk, 0);
	duckdb_data_chunk_ensure_validity_writable(data_chunk, 1);
	auto col1_validity = duckdb_data_chunk_get_validity(data_chunk, 0);
	REQUIRE(col1_validity);
	auto col2_validity = duckdb_data_chunk_get_validity(data_chunk, 1);
	REQUIRE(col2_validity);
	*col1_validity = 0;
	*col2_validity = 0;

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);

	REQUIRE(duckdb_data_chunk_get_validity(nullptr, 0) == nullptr);
	REQUIRE(duckdb_data_chunk_get_validity(data_chunk, 999) == nullptr);

	duckdb_appender_destroy(&appender);

	result = tester.Query("SELECT * FROM test");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int16_t>(1, 0) == 84);
	REQUIRE(result->IsNull(0, 1));
	REQUIRE(result->IsNull(1, 1));

	duckdb_data_chunk_reset(data_chunk);
	duckdb_data_chunk_reset(nullptr);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(&data_chunk);

	duckdb_destroy_data_chunk(nullptr);

	duckdb_destroy_logical_type(&types[0]);
	duckdb_destroy_logical_type(&types[1]);
}
