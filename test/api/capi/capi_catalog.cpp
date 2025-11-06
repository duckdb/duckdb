#include "capi_tester.hpp"
#include "duckdb.h"

#include <cstring> // for strcmp

using namespace duckdb;
using namespace std;

//----------------------------------------------------------------------------------------------------------------------
// Test Catalog Interface in C API
//----------------------------------------------------------------------------------------------------------------------

static void Execute(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	auto context = (duckdb_client_context)duckdb_scalar_function_get_extra_info(info);

	duckdb_catalog catalog = duckdb_client_context_get_catalog(context, "memory");
	if (catalog == nullptr) {
		duckdb_scalar_function_set_error(info, "Could not get catalog");
		return;
	}

	// Verify that this is a duck catalog
	auto catalog_type = duckdb_catalog_get_type(catalog);
	if (strcmp(catalog_type, "duckdb") != 0) {
		duckdb_destroy_catalog(&catalog);
		duckdb_scalar_function_set_error(info, "Catalog type is not duckdb");
		return;
	}

	// Get first argument (schema name)
	auto input_size = duckdb_data_chunk_get_size(input);

	auto schema_vector = duckdb_data_chunk_get_vector(input, 0);
	auto schema_data = (duckdb_string_t *)duckdb_vector_get_data(schema_vector);

	auto name_vector = duckdb_data_chunk_get_vector(input, 1);
	auto name_data = (duckdb_string_t *)duckdb_vector_get_data(name_vector);

	auto result_data = (bool *)duckdb_vector_get_data(output);

	for (idx_t row_idx = 0; row_idx < input_size; row_idx++) {
		duckdb_catalog_entry entry;
		duckdb_string_t schema_str = schema_data[row_idx];
		duckdb_string_t name_str = name_data[row_idx];

		string schema(duckdb_string_t_data(&schema_str), duckdb_string_t_length(schema_str));
		string name(duckdb_string_t_data(&name_str), duckdb_string_t_length(name_str));

		entry =
		    duckdb_catalog_get_entry(catalog, context, DUCKDB_CATALOG_ENTRY_TYPE_TABLE, schema.c_str(), name.c_str());

		result_data[row_idx] = (entry != nullptr);
		duckdb_destroy_catalog_entry(&entry);
		duckdb_destroy_catalog_entry(&entry);
	}

	duckdb_destroy_catalog(&catalog);
	duckdb_destroy_catalog(&catalog);
}

TEST_CASE("Test Catalog Interface in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	// Get the client context to pass as extra info
	duckdb_client_context context;
	duckdb_connection_get_client_context(tester.connection, &context);

	// We need a scalar function to get the catalog (from within a transaction)
	auto func = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(func, "test_lookup_function");
	auto arg_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	auto ret_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
	duckdb_scalar_function_add_parameter(func, arg_type);
	duckdb_scalar_function_add_parameter(func, arg_type);
	duckdb_scalar_function_set_return_type(func, ret_type);
	duckdb_scalar_function_set_function(func, Execute);
	duckdb_scalar_function_set_extra_info(func, context, nullptr);

	REQUIRE(duckdb_register_scalar_function(tester.connection, func) == DuckDBSuccess);
	duckdb_destroy_scalar_function(&func);
	duckdb_destroy_logical_type(&ret_type);
	duckdb_destroy_logical_type(&arg_type);

	// Execute a query to test the catalog interface with a non-existing table
	result = tester.Query("SELECT test_lookup_function('main', 'test_table') AS exists;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<bool>(0, 0) == false);

	// Now create a table and test again
	REQUIRE_NO_FAIL(tester.Query("CREATE TABLE test_table(i INTEGER);"));

	result = tester.Query("SELECT test_lookup_function('main', 'test_table') AS exists;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<bool>(0, 0) == true);
}
