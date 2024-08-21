#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

static duckdb_custom_type CAPIGetCustomType(const char *name, duckdb_type duckdb_type) {
	auto base_type = duckdb_create_logical_type(duckdb_type);
	duckdb_logical_type_set_alias(base_type, name);

	auto custom_type = duckdb_create_custom_type();
	duckdb_custom_type_set_name(custom_type, name);
	duckdb_custom_type_set_base_type(custom_type, base_type);

	duckdb_destroy_logical_type(&base_type);

	return custom_type;
}

static void CAPIRegisterCustomType(duckdb_connection connection, const char *name, duckdb_type duckdb_type,
                                   duckdb_state expected_outcome) {
	duckdb_state status;

	auto custom_type = CAPIGetCustomType(name, duckdb_type);

	status = duckdb_register_custom_type(connection, custom_type);
	REQUIRE(status == expected_outcome);

	duckdb_destroy_custom_type(&custom_type);
	duckdb_destroy_custom_type(&custom_type);
	duckdb_destroy_custom_type(nullptr);
}

TEST_CASE("Test Custom Type Registration", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	// try to register a custom type with an invalid base type
	CAPIRegisterCustomType(tester.connection, "NUMBER", DUCKDB_TYPE_INVALID, DuckDBError);
	// try to register a custom type with a valid base type
	CAPIRegisterCustomType(tester.connection, "NUMBER", DUCKDB_TYPE_INTEGER, DuckDBSuccess);
	// try to register it again - this should be an error
	CAPIRegisterCustomType(tester.connection, "NUMBER", DUCKDB_TYPE_INTEGER, DuckDBError);

	// check that it is in the catalog
	result = tester.Query("SELECT type_name FROM duckdb_types WHERE type_name = 'NUMBER'");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<string>(0, 0) == "NUMBER");
}

TEST_CASE("Test Custom Type Function", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	// Register a custom type (VEC3D) that is between 0 and 1
	auto element_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
	auto vector_type = duckdb_create_array_type(element_type, 3);
	duckdb_logical_type_set_alias(vector_type, "VEC3D");

	auto custom_type = duckdb_create_custom_type();
	duckdb_custom_type_set_name(custom_type, "VEC3D");
	duckdb_custom_type_set_base_type(custom_type, vector_type);

	REQUIRE(duckdb_register_custom_type(tester.connection, custom_type) == DuckDBSuccess);

	// Register a scalar function that adds two vectors
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "vec3d_add");
	duckdb_scalar_function_set_return_type(function, vector_type);
	duckdb_scalar_function_add_parameter(function, vector_type);
	duckdb_scalar_function_add_parameter(function, vector_type);
	duckdb_scalar_function_set_function(function, [](duckdb_function_info info, duckdb_data_chunk input,
	                                                 duckdb_vector output) {
		const auto count = duckdb_data_chunk_get_size(input);
		const auto left_vector = duckdb_data_chunk_get_vector(input, 0);
		const auto right_vector = duckdb_data_chunk_get_vector(input, 1);
		const auto left_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(left_vector)));
		const auto right_data =
		    static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(right_vector)));
		const auto result_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(output)));

		for (idx_t i = 0; i < count; i++) {
			for (idx_t j = 0; j < 3; j++) {
				const auto idx = i * 3 + j;
				result_data[idx] = left_data[idx] + right_data[idx];
			}
		}
	});

	REQUIRE(duckdb_register_scalar_function(tester.connection, function) == DuckDBSuccess);

	// Cleanup the custom type and scalar function
	duckdb_destroy_scalar_function(&function);
	duckdb_destroy_logical_type(&element_type);
	duckdb_destroy_logical_type(&vector_type);
	duckdb_destroy_custom_type(&custom_type);

	// Create a table with the custom type
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE vec3d_table (a VEC3D)"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO vec3d_table VALUES ([1.0, 2.0, 3.0]::FLOAT[3])"));
	REQUIRE_NO_FAIL(tester.Query("INSERT INTO vec3d_table VALUES ([4.0, 5.0, 6.0]::FLOAT[3])"));

	// Query the table
	result = tester.Query("SELECT vec3d_add(a, a) FROM vec3d_table");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 2);

	// Check that the result is correct
	auto chunk = result->FetchChunk(0);
	auto data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(chunk->GetVector(0))));
	REQUIRE(data[0] == 2.0f);
	REQUIRE(data[1] == 4.0f);
	REQUIRE(data[2] == 6.0f);
	REQUIRE(data[3] == 8.0f);
	REQUIRE(data[4] == 10.0f);
	REQUIRE(data[5] == 12.0f);

	// But we cant execute the function with a non-VEC3D type
	result = tester.Query("SELECT vec3d_add([0,0,0]::FLOAT[3], [1,1,1]::FLOAT[3])");
	REQUIRE_FAIL(result);
	REQUIRE(result->ErrorType() == DUCKDB_ERROR_BINDER);

	// But we can cast the base type to the custom type
	result = tester.Query("SELECT vec3d_add(CAST([0,0,0] AS VEC3D), CAST([1,1,1] AS VEC3D))");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 1);
}
