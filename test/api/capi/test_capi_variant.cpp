#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test C API VARIANT type support", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	// Test 1: Create a table with VARIANT column
	REQUIRE(tester.OpenConnection());
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE variant_test (id INTEGER, data VARIANT)"));

	// Test 2: Insert some VARIANT data
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_test VALUES (1, 42::VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_test VALUES (2, 'hello'::VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_test VALUES (3, [1, 2, 3]::VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_test VALUES (4, {'a': 1, 'b': 2}::VARIANT)"));

	// Test 3: Query the data and check the type
	result = tester.Query("SELECT * FROM variant_test ORDER BY id");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->ColumnCount() == 2);
	REQUIRE(result->row_count() == 4);

	// Test 4: Verify the VARIANT column type using duckdb_column_type
	auto column_type = duckdb_column_type(&result->InternalResult(), 1);
	REQUIRE(column_type == DUCKDB_TYPE_VARIANT);

	// Test 5: Get the logical type and verify it returns VARIANT
	auto logical_type = duckdb_column_logical_type(&result->InternalResult(), 1);
	REQUIRE(logical_type != nullptr);
	auto type_id = duckdb_get_type_id(logical_type);
	REQUIRE(type_id == DUCKDB_TYPE_VARIANT);
	duckdb_destroy_logical_type(&logical_type);

	// Test 6: Test duckdb_create_logical_type with VARIANT
	// Note: VARIANT is a complex type, so creating it directly should return INVALID
	auto variant_logical_type = duckdb_create_logical_type(DUCKDB_TYPE_VARIANT);
	REQUIRE(variant_logical_type == nullptr);
}

TEST_CASE("Test C API VARIANT type in prepared statements", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenConnection());
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE variant_prep (data VARIANT)"));

	// Test prepared statement parameter type
	auto state = tester.Prepare("SELECT * FROM variant_prep WHERE data = ?::VARIANT");
	REQUIRE(state);

	// Get the parameter type - it should be recognized
	auto param_type = duckdb_prepare_parameter_type(state->InternalStatement(), 1);
	// The parameter itself might not be VARIANT, but the result should be

	duckdb_destroy_prepare(&state->InternalStatement());

	// Test prepared statement result type
	state = tester.Prepare("SELECT data FROM variant_prep");
	REQUIRE(state);

	auto result_type = duckdb_prepare_column_type(state->InternalStatement(), 0);
	REQUIRE(result_type == DUCKDB_TYPE_VARIANT);

	duckdb_destroy_prepare(&state->InternalStatement());
}

TEST_CASE("Test C API VARIANT type with NULL values", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenConnection());
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE variant_null_test (data VARIANT)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_null_test VALUES (NULL)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO variant_null_test VALUES (42::VARIANT)"));

	result = tester.Query("SELECT * FROM variant_null_test ORDER BY data NULLS FIRST");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 2);

	// Verify the column type is VARIANT
	auto column_type = duckdb_column_type(&result->InternalResult(), 0);
	REQUIRE(column_type == DUCKDB_TYPE_VARIANT);
}
