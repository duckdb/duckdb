#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test logical type creation with unsupported types", "[capi]") {
	// Test duckdb_create_logical_type with unsupported types.
	duckdb::vector<duckdb_type> unsupported_types = {
	    DUCKDB_TYPE_INVALID, DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_ENUM,  DUCKDB_TYPE_LIST,
	    DUCKDB_TYPE_STRUCT,  DUCKDB_TYPE_MAP,     DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_UNION,
	};

	for (const auto unsupported_type : unsupported_types) {
		auto logical_type = duckdb_create_logical_type(unsupported_type);
		REQUIRE(DUCKDB_TYPE_INVALID == duckdb_get_type_id(logical_type));
		duckdb_destroy_logical_type(&logical_type);
	}
}

TEST_CASE("Test INVALID, ANY and SQLNULL", "[capi]") {
	auto sql_null_type = duckdb_create_logical_type(DUCKDB_TYPE_SQLNULL);
	duckdb_destroy_logical_type(&sql_null_type);

	auto any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);

	auto result_type_id = duckdb_get_type_id(any_type);
	REQUIRE(result_type_id == DUCKDB_TYPE_ANY);
	result_type_id = duckdb_get_type_id(invalid_type);
	REQUIRE(result_type_id == DUCKDB_TYPE_INVALID);

	// LIST with ANY
	auto list = duckdb_create_list_type(any_type);
	result_type_id = duckdb_get_type_id(list);
	REQUIRE(result_type_id == DUCKDB_TYPE_LIST);
	duckdb_destroy_logical_type(&list);

	// LIST with INVALID
	list = duckdb_create_list_type(invalid_type);
	result_type_id = duckdb_get_type_id(list);
	REQUIRE(result_type_id == DUCKDB_TYPE_LIST);
	duckdb_destroy_logical_type(&list);

	// ARRAY with ANY
	auto array = duckdb_create_array_type(any_type, 2);
	result_type_id = duckdb_get_type_id(array);
	REQUIRE(result_type_id == DUCKDB_TYPE_ARRAY);
	duckdb_destroy_logical_type(&array);

	// ARRAY with INVALID
	array = duckdb_create_array_type(invalid_type, 2);
	result_type_id = duckdb_get_type_id(array);
	REQUIRE(result_type_id == DUCKDB_TYPE_ARRAY);
	duckdb_destroy_logical_type(&array);

	// MAP with ANY
	auto map = duckdb_create_map_type(any_type, any_type);
	result_type_id = duckdb_get_type_id(map);
	REQUIRE(result_type_id == DUCKDB_TYPE_MAP);
	duckdb_destroy_logical_type(&map);

	// MAP with INVALID
	map = duckdb_create_map_type(any_type, any_type);
	result_type_id = duckdb_get_type_id(map);
	REQUIRE(result_type_id == DUCKDB_TYPE_MAP);
	duckdb_destroy_logical_type(&map);

	// UNION with ANY and INVALID
	std::vector<const char *> member_names {"any", "invalid"};
	duckdb::vector<duckdb_logical_type> types = {any_type, invalid_type};
	auto union_type = duckdb_create_union_type(types.data(), member_names.data(), member_names.size());
	result_type_id = duckdb_get_type_id(union_type);
	REQUIRE(result_type_id == DUCKDB_TYPE_UNION);
	duckdb_destroy_logical_type(&union_type);

	// Clean-up.
	duckdb_destroy_logical_type(&any_type);
	duckdb_destroy_logical_type(&invalid_type);
}

TEST_CASE("Test LIST and ARRAY with INVALID and ANY", "[capi]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	auto any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);

	auto value = duckdb_create_int64(42);
	duckdb::vector<duckdb_value> list_values {value, value};

	auto int_list = duckdb_create_list_value(int_type, list_values.data(), list_values.size());
	auto result = duckdb_get_varchar(int_list);
	REQUIRE(string(result).compare("[42, 42]") == 0);
	duckdb_free(result);
	duckdb_destroy_value(&int_list);

	auto int_array = duckdb_create_array_value(int_type, list_values.data(), list_values.size());
	result = duckdb_get_varchar(int_array);
	REQUIRE(string(result).compare("[42, 42]") == 0);
	duckdb_free(result);
	duckdb_destroy_value(&int_array);

	auto invalid_list = duckdb_create_list_value(any_type, list_values.data(), list_values.size());
	REQUIRE(invalid_list == nullptr);
	auto invalid_array = duckdb_create_array_value(any_type, list_values.data(), list_values.size());
	REQUIRE(invalid_array == nullptr);
	auto any_list = duckdb_create_list_value(any_type, list_values.data(), list_values.size());
	REQUIRE(any_list == nullptr);
	auto any_array = duckdb_create_array_value(any_type, list_values.data(), list_values.size());
	REQUIRE(any_array == nullptr);

	// Clean-up.
	duckdb_destroy_value(&value);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&any_type);
	duckdb_destroy_logical_type(&invalid_type);
}

TEST_CASE("Test STRUCT with INVALID and ANY", "[capi]") {
	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	auto any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);

	auto value = duckdb_create_int64(42);
	duckdb::vector<duckdb_value> struct_values {value, value};

	// Test duckdb_create_struct_type with ANY.
	std::vector<const char *> member_names {"int", "other"};
	duckdb::vector<duckdb_logical_type> types = {int_type, any_type};
	auto struct_type = duckdb_create_struct_type(types.data(), member_names.data(), member_names.size());
	REQUIRE(struct_type != nullptr);

	// Test duckdb_create_struct_value with ANY.
	auto struct_value = duckdb_create_struct_value(struct_type, struct_values.data());
	REQUIRE(struct_value == nullptr);
	duckdb_destroy_logical_type(&struct_type);

	// Test duckdb_create_struct_type with INVALID.
	types = {int_type, invalid_type};
	struct_type = duckdb_create_struct_type(types.data(), member_names.data(), member_names.size());
	REQUIRE(struct_type != nullptr);

	// Test duckdb_create_struct_value with INVALID.
	struct_value = duckdb_create_struct_value(struct_type, struct_values.data());
	REQUIRE(struct_value == nullptr);
	duckdb_destroy_logical_type(&struct_type);

	// Clean-up.
	duckdb_destroy_value(&value);
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&any_type);
	duckdb_destroy_logical_type(&invalid_type);
}

TEST_CASE("Test data chunk creation with INVALID and ANY types", "[capi]") {
	auto any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);
	auto list_type = duckdb_create_list_type(any_type);

	// For each type, try to create a data chunk with that type.
	std::vector<duckdb_logical_type> test_types = {any_type, invalid_type, list_type};
	for (idx_t i = 0; i < test_types.size(); i++) {
		duckdb_logical_type types[1];
		types[0] = test_types[i];
		auto data_chunk = duckdb_create_data_chunk(types, 1);
		REQUIRE(data_chunk == nullptr);
	}

	// Clean-up.
	duckdb_destroy_logical_type(&list_type);
	duckdb_destroy_logical_type(&any_type);
	duckdb_destroy_logical_type(&invalid_type);
}

void DummyScalar(duckdb_function_info, duckdb_data_chunk, duckdb_vector) {
}

static duckdb_scalar_function DummyScalarFunction() {
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "hello");
	duckdb_scalar_function_set_function(function, DummyScalar);
	return function;
}

static void TestScalarFunction(duckdb_scalar_function function, duckdb_connection connection) {
	auto status = duckdb_register_scalar_function(connection, function);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_scalar_function(&function);
}

TEST_CASE("Test scalar functions with INVALID and ANY types", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	auto int_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	auto any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);

	// Set INVALID as a parameter.
	auto function = DummyScalarFunction();
	duckdb_scalar_function_add_parameter(function, invalid_type);
	duckdb_scalar_function_set_return_type(function, int_type);
	TestScalarFunction(function, tester.connection);

	// Set INVALID as the return type.
	function = DummyScalarFunction();
	duckdb_scalar_function_set_return_type(function, invalid_type);
	TestScalarFunction(function, tester.connection);

	// Set ANY as the return type.
	function = DummyScalarFunction();
	duckdb_scalar_function_set_return_type(function, any_type);
	TestScalarFunction(function, tester.connection);

	// Clean-up.
	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&any_type);
	duckdb_destroy_logical_type(&invalid_type);
}

void my_dummy_bind(duckdb_bind_info) {
}

void my_dummy_init(duckdb_init_info) {
}

void my_dummy_function(duckdb_function_info, duckdb_data_chunk) {
}

static duckdb_table_function DummyTableFunction() {
	auto function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, "hello");
	duckdb_table_function_set_bind(function, my_dummy_bind);
	duckdb_table_function_set_init(function, my_dummy_init);
	duckdb_table_function_set_function(function, my_dummy_function);
	return function;
}

static void TestTableFunction(duckdb_table_function function, duckdb_connection connection) {
	auto status = duckdb_register_table_function(connection, function);
	REQUIRE(status == DuckDBError);
	duckdb_destroy_table_function(&function);
}

TEST_CASE("Test table functions with INVALID and ANY types", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	REQUIRE(tester.OpenDatabase(nullptr));

	auto invalid_type = duckdb_create_logical_type(DUCKDB_TYPE_INVALID);

	// Set INVALID as a parameter.
	auto function = DummyTableFunction();
	duckdb_table_function_add_parameter(function, invalid_type);
	TestTableFunction(function, tester.connection);

	// Set INVALID as a named parameter.
	function = DummyTableFunction();
	duckdb_table_function_add_named_parameter(function, "my_parameter", invalid_type);
	TestTableFunction(function, tester.connection);

	duckdb_destroy_logical_type(&invalid_type);
}
