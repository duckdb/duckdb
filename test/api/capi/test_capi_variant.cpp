#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test C API VARIANT type support", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE t1 (data VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO t1 VALUES (42::VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO t1 VALUES ('hello world'::VARIANT)"));

	result = tester.Query("SELECT data FROM t1");
	REQUIRE_NO_FAIL(*result);

	REQUIRE(result->ColumnType(0) == DUCKDB_TYPE_VARIANT);

	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	auto vec = duckdb_data_chunk_get_vector(chunk->GetChunk(), 0);

	auto type = duckdb_vector_get_column_type(vec);

	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_VARIANT);

	duckdb_destroy_logical_type(&type);
}

TEST_CASE("Test C API create VARIANT logical type", "[capi]") {
	auto type = duckdb_create_logical_type(DUCKDB_TYPE_VARIANT);

	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_VARIANT);

	auto vector_handle = duckdb_create_vector(type, 1);
	REQUIRE(vector_handle);

	auto vector_type = duckdb_vector_get_column_type(vector_handle);
	REQUIRE(duckdb_get_type_id(vector_type) == DUCKDB_TYPE_VARIANT);

	duckdb_destroy_logical_type(&vector_type);
	duckdb_destroy_vector(&vector_handle);
	duckdb_destroy_logical_type(&type);
}
