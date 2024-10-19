#include "capi_tester.hpp"
#include <cstdio>

using namespace duckdb;
using namespace std;

static void CAPIRegisterCustomType(duckdb_connection connection, const char *name, duckdb_type duckdb_type,
                                   duckdb_state expected_outcome) {
	duckdb_state status;

	auto base_type = duckdb_create_logical_type(duckdb_type);
	duckdb_logical_type_set_alias(base_type, name);

	status = duckdb_register_logical_type(connection, base_type, nullptr);
	REQUIRE(status == expected_outcome);

	duckdb_destroy_logical_type(&base_type);
	duckdb_destroy_logical_type(&base_type);
	duckdb_destroy_logical_type(nullptr);
}

static void Vec3DAddFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	const auto count = duckdb_data_chunk_get_size(input);
	const auto left_vector = duckdb_data_chunk_get_vector(input, 0);
	const auto right_vector = duckdb_data_chunk_get_vector(input, 1);
	const auto left_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(left_vector)));
	const auto right_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(right_vector)));
	const auto result_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(output)));

	for (idx_t i = 0; i < count; i++) {
		for (idx_t j = 0; j < 3; j++) {
			const auto idx = i * 3 + j;
			result_data[idx] = left_data[idx] + right_data[idx];
		}
	}
}

static bool Vec3DToVarcharCastFunction(duckdb_function_info info, idx_t count, duckdb_vector input,
                                       duckdb_vector output) {
	const auto input_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(input)));

	for (idx_t i = 0; i < count; i++) {
		const auto x = input_data[i * 3];
		const auto y = input_data[i * 3 + 1];
		const auto z = input_data[i * 3 + 2];
		const auto res = StringUtil::Format("<%f, %f, %f>", x, y, z);
		duckdb_vector_assign_string_element_len(output, i, res.c_str(), res.size());
	}
	return true;
}

static bool TryParseVec3D(char *str, idx_t len, float &x, float &y, float &z) {
	char *end = str + len;
	while (str < end && *str != '<') {
		str++;
	}
	str++;
	if (str >= end) {
		return false;
	}
	x = std::strtof(str, &str);
	if (str >= end || *str != ',') {
		return false;
	}
	str++;
	y = std::strtof(str, &str);
	if (str >= end || *str != ',') {
		return false;
	}
	str++;
	z = std::strtof(str, &str);
	if (str >= end || *str != '>') {
		return false;
	}
	str++;
	return str == end;
}

static bool Vec3DFromVarcharCastFunction(duckdb_function_info info, idx_t count, duckdb_vector input,
                                         duckdb_vector output) {
	const auto cast_mode = duckdb_cast_function_get_cast_mode(info);

	// For testing purposes, check that we got the custom data
	auto custom_data = reinterpret_cast<string *>(duckdb_cast_function_get_extra_info(info));
	REQUIRE(*custom_data == "foobar");

	const auto input_data = static_cast<duckdb_string_t *>(duckdb_vector_get_data(input));
	const auto output_data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(output)));

	bool success = true;
	for (idx_t i = 0; i < count; i++) {
		auto x = 0.0f, y = 0.0f, z = 0.0f;
		auto str = input_data[i];
		auto str_len = duckdb_string_t_length(str);
		char *str_ptr = duckdb_string_is_inlined(str) ? str.value.inlined.inlined : str.value.pointer.ptr;

		// yes, sscanf is not safe, but this is just a test
		if (TryParseVec3D(str_ptr, str_len, x, y, z)) {
			// Success
			output_data[i * 3] = x;
			output_data[i * 3 + 1] = y;
			output_data[i * 3 + 2] = z;
		} else {
			// Error
			duckdb_cast_function_set_row_error(info, "Failed to parse VEC3D", i, output);
			if (cast_mode == DUCKDB_CAST_TRY) {
				// Try cast, continue with the next row
				success = false;
			} else {
				// Strict cast, short-circuit and return false
				return false;
			}
		}
	}

	return success;
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

	REQUIRE(duckdb_register_logical_type(tester.connection, vector_type, nullptr) == DuckDBSuccess);

	// Register a scalar function that adds two vectors
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "vec3d_add");
	duckdb_scalar_function_set_return_type(function, vector_type);
	duckdb_scalar_function_add_parameter(function, vector_type);
	duckdb_scalar_function_add_parameter(function, vector_type);
	duckdb_scalar_function_set_function(function, Vec3DAddFunction);

	REQUIRE(duckdb_register_scalar_function(tester.connection, function) == DuckDBSuccess);

	// Also add a cast function to convert VEC3D to VARCHAR
	auto varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

	auto to_varchar_cast_function = duckdb_create_cast_function();
	duckdb_cast_function_set_implicit_cast_cost(to_varchar_cast_function, 0);
	duckdb_cast_function_set_source_type(to_varchar_cast_function, vector_type);
	duckdb_cast_function_set_target_type(to_varchar_cast_function, varchar_type);
	duckdb_cast_function_set_function(to_varchar_cast_function, Vec3DToVarcharCastFunction);

	REQUIRE(duckdb_register_cast_function(tester.connection, to_varchar_cast_function) == DuckDBSuccess);

	auto from_varchar_cast_function = duckdb_create_cast_function();
	duckdb_cast_function_set_implicit_cast_cost(from_varchar_cast_function, 0);
	duckdb_cast_function_set_source_type(from_varchar_cast_function, varchar_type);
	duckdb_cast_function_set_target_type(from_varchar_cast_function, vector_type);
	duckdb_cast_function_set_function(from_varchar_cast_function, Vec3DFromVarcharCastFunction);

	auto cast_custom_data = new string("foobar");
	auto cast_custom_data_delete = [](void *data) {
		delete reinterpret_cast<string *>(data);
	};
	duckdb_cast_function_set_extra_info(from_varchar_cast_function, cast_custom_data, cast_custom_data_delete);

	REQUIRE(duckdb_register_cast_function(tester.connection, from_varchar_cast_function) == DuckDBSuccess);

	// Cleanup the custom type and functions
	duckdb_destroy_scalar_function(&function);
	duckdb_destroy_cast_function(&to_varchar_cast_function);
	duckdb_destroy_cast_function(&from_varchar_cast_function);
	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&element_type);
	duckdb_destroy_logical_type(&vector_type);

	// Ensure that we can free the casts multiple times without issue
	duckdb_destroy_cast_function(&from_varchar_cast_function);
	duckdb_destroy_cast_function(nullptr);

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

	// Speaking of casts, let's test the VEC3D to VARCHAR cast
	result = tester.Query("SELECT CAST(a AS VARCHAR) FROM vec3d_table");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 2);
	// Check that the result is correct
	REQUIRE(result->Fetch<string>(0, 0) == "<1.000000, 2.000000, 3.000000>");
	REQUIRE(result->Fetch<string>(0, 1) == "<4.000000, 5.000000, 6.000000>");

	// Now cast from varchar to VEC3D
	result = tester.Query("SELECT CAST('<1.0, 3.0, 4.0>' AS VEC3D)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 1);

	// Check that the result is correct
	chunk = result->FetchChunk(0);
	data = static_cast<float *>(duckdb_vector_get_data(duckdb_array_vector_get_child(chunk->GetVector(0))));
	REQUIRE(data[0] == 1.0f);
	REQUIRE(data[1] == 3.0f);
	REQUIRE(data[2] == 4.0f);

	// Try a faulty cast
	result = tester.Query("SELECT CAST('<1.0, 3.0, abc' AS VEC3D)");
	REQUIRE_FAIL(result);
	REQUIRE(result->ErrorType() == DUCKDB_ERROR_CONVERSION);
	REQUIRE_THAT(result->ErrorMessage(), Catch::Matchers::StartsWith("Conversion Error: Failed to parse VEC3D"));

	// Try a faulty cast with TRY_CAST
	result = tester.Query("SELECT TRY_CAST('<1.0, 3.0, abc' AS FLOAT[3]) IS NULL");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 1);
	REQUIRE(result->Fetch<bool>(0, 0) == true);
}
