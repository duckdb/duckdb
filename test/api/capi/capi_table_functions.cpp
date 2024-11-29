#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

struct my_bind_data_struct {
	int64_t size;
};

void my_bind(duckdb_bind_info info) {
	REQUIRE(duckdb_bind_get_parameter_count(info) == 1);

	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "forty_two", type);
	duckdb_destroy_logical_type(&type);

	auto my_bind_data = (my_bind_data_struct *)malloc(sizeof(my_bind_data_struct));
	auto param = duckdb_bind_get_parameter(info, 0);
	my_bind_data->size = duckdb_get_int64(param);
	duckdb_destroy_value(&param);

	duckdb_bind_set_bind_data(info, my_bind_data, free);
}

struct my_init_data_struct {
	int64_t pos;
};

void my_init(duckdb_init_info info) {
	REQUIRE(duckdb_init_get_bind_data(info) != nullptr);
	REQUIRE(duckdb_init_get_bind_data(nullptr) == nullptr);

	auto my_init_data = (my_init_data_struct *)malloc(sizeof(my_init_data_struct));
	my_init_data->pos = 0;
	duckdb_init_set_init_data(info, my_init_data, free);
}

void my_function(duckdb_function_info info, duckdb_data_chunk output) {
	auto bind_data = (my_bind_data_struct *)duckdb_function_get_bind_data(info);
	auto init_data = (my_init_data_struct *)duckdb_function_get_init_data(info);
	auto ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
	idx_t i;
	for (i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (init_data->pos >= bind_data->size) {
			break;
		}
		ptr[i] = init_data->pos % 2 == 0 ? 42 : 84;
		init_data->pos++;
	}
	duckdb_data_chunk_set_size(output, i);
}

static void capi_register_table_function(duckdb_connection connection, const char *name,
                                         duckdb_table_function_bind_t bind, duckdb_table_function_init_t init,
                                         duckdb_table_function_t f, duckdb_state expected_state = DuckDBSuccess) {
	duckdb_state status;
	// create a table function
	auto function = duckdb_create_table_function();
	duckdb_table_function_set_name(nullptr, name);
	duckdb_table_function_set_name(function, nullptr);
	duckdb_table_function_set_name(function, name);
	duckdb_table_function_set_name(function, name);

	// add a string parameter
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_table_function_add_parameter(function, type);
	duckdb_destroy_logical_type(&type);

	// add a named parameter
	duckdb_logical_type itype = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_table_function_add_named_parameter(function, "my_parameter", itype);
	duckdb_destroy_logical_type(&itype);

	// set up the function pointers
	duckdb_table_function_set_bind(function, bind);
	duckdb_table_function_set_init(function, init);
	duckdb_table_function_set_function(function, f);

	// register and cleanup
	status = duckdb_register_table_function(connection, function);
	duckdb_destroy_table_function(&function);
	duckdb_destroy_table_function(&function);
	duckdb_destroy_table_function(nullptr);
	REQUIRE(status == expected_state);
}

TEST_CASE("Test Table Functions C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	capi_register_table_function(tester.connection, "my_function", my_bind, my_init, my_function);
	// registering again causes an error
	capi_register_table_function(tester.connection, "my_function", my_bind, my_init, my_function, DuckDBError);

	// now call it
	result = tester.Query("SELECT * FROM my_function(1)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);

	result = tester.Query("SELECT * FROM my_function(1, my_parameter=3)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);

	result = tester.Query("SELECT * FROM my_function(1, my_parameter=\"val\")");
	REQUIRE(result->HasError());
	result = tester.Query("SELECT * FROM my_function(1, nota_parameter=\"val\")");
	REQUIRE(result->HasError());

	result = tester.Query("SELECT * FROM my_function(3)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 84);
	REQUIRE(result->Fetch<int64_t>(0, 2) == 42);

	result = tester.Query("SELECT forty_two, COUNT(*) FROM my_function(10000) GROUP BY 1 ORDER BY 1");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 84);
	REQUIRE(result->Fetch<int64_t>(1, 0) == 5000);
	REQUIRE(result->Fetch<int64_t>(1, 1) == 5000);
}

void my_error_bind(duckdb_bind_info info) {
	duckdb_bind_set_error(nullptr, nullptr);
	duckdb_bind_set_error(info, "My error message");
}

void my_error_init(duckdb_init_info info) {
	duckdb_init_set_error(nullptr, nullptr);
	duckdb_init_set_error(info, "My error message");
}

void my_error_function(duckdb_function_info info, duckdb_data_chunk output) {
	duckdb_function_set_error(nullptr, nullptr);
	duckdb_function_set_error(info, "My error message");
}

TEST_CASE("Test Table Function errors in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	capi_register_table_function(tester.connection, "my_error_bind", my_error_bind, my_init, my_function);
	capi_register_table_function(tester.connection, "my_error_init", my_bind, my_error_init, my_function);
	capi_register_table_function(tester.connection, "my_error_function", my_bind, my_init, my_error_function);

	result = tester.Query("SELECT * FROM my_error_bind(1)");
	REQUIRE(result->HasError());
	result = tester.Query("SELECT * FROM my_error_init(1)");
	REQUIRE(result->HasError());
	result = tester.Query("SELECT * FROM my_error_function(1)");
	REQUIRE(result->HasError());
}

TEST_CASE("Test Table Function register errors in C API", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	capi_register_table_function(tester.connection, "x", my_error_bind, my_init, my_function, DuckDBSuccess);
	// Try to register it again with the same name, name collision
	capi_register_table_function(tester.connection, "x", my_error_bind, my_init, my_function, DuckDBError);
}

struct my_named_bind_data_struct {
	int64_t size;
	int64_t multiplier;
};

void my_named_bind(duckdb_bind_info info) {
	REQUIRE(duckdb_bind_get_parameter_count(info) == 1);

	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "forty_two", type);
	duckdb_destroy_logical_type(&type);

	auto my_bind_data = (my_named_bind_data_struct *)malloc(sizeof(my_named_bind_data_struct));

	auto param = duckdb_bind_get_parameter(info, 0);
	my_bind_data->size = duckdb_get_int64(param);
	duckdb_destroy_value(&param);

	auto nparam = duckdb_bind_get_named_parameter(info, "my_parameter");
	if (nparam) {
		my_bind_data->multiplier = duckdb_get_int64(nparam);
	} else {
		my_bind_data->multiplier = 1;
	}
	duckdb_destroy_value(&nparam);

	duckdb_bind_set_bind_data(info, my_bind_data, free);
}

void my_named_init(duckdb_init_info info) {
	REQUIRE(duckdb_init_get_bind_data(info) != nullptr);
	REQUIRE(duckdb_init_get_bind_data(nullptr) == nullptr);

	auto my_init_data = (my_init_data_struct *)malloc(sizeof(my_init_data_struct));
	my_init_data->pos = 0;
	duckdb_init_set_init_data(info, my_init_data, free);
}

void my_named_function(duckdb_function_info info, duckdb_data_chunk output) {
	auto bind_data = (my_named_bind_data_struct *)duckdb_function_get_bind_data(info);
	auto init_data = (my_init_data_struct *)duckdb_function_get_init_data(info);
	auto ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
	idx_t i;
	for (i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (init_data->pos >= bind_data->size) {
			break;
		}
		ptr[i] = init_data->pos % 2 == 0 ? (42 * bind_data->multiplier) : (84 * bind_data->multiplier);
		init_data->pos++;
	}
	duckdb_data_chunk_set_size(output, i);
}

TEST_CASE("Test Table Function named parameters in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	capi_register_table_function(tester.connection, "my_multiplier_function", my_named_bind, my_named_init,
	                             my_named_function);

	result = tester.Query("SELECT * FROM my_multiplier_function(3)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 84);
	REQUIRE(result->Fetch<int64_t>(0, 2) == 42);

	result = tester.Query("SELECT * FROM my_multiplier_function(2, my_parameter=2)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 84);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 168);

	result = tester.Query("SELECT * FROM my_multiplier_function(2, my_parameter=3)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 126);
	REQUIRE(result->Fetch<int64_t>(0, 1) == 252);
}

void fltr_bind(duckdb_bind_info info) {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "i", type);
	duckdb_destroy_logical_type(&type);

	duckdb_bind_set_bind_data(info, nullptr, free);
}

void fltr_init(duckdb_init_info info) {
	duckdb_table_filters filters = duckdb_init_get_table_filters(info);
	REQUIRE(filters != nullptr);
	// TODO: test case where there are no filters

	// Expect one filter
	idx_t nfilters = duckdb_table_filters_size(filters);
	REQUIRE(nfilters == 1);

	// Get the filter
	duckdb_table_filter tfltr = duckdb_table_filters_get_filter(filters, 0);
	REQUIRE(tfltr != nullptr);

	// Expect nullptr for an invalid filter index
	duckdb_table_filter tfltr_not = duckdb_table_filters_get_filter(filters, 1);
	REQUIRE(tfltr_not == nullptr);

	// Expect a conjunction-and filter
	duckdb_table_filter_type fltr_type = duckdb_table_filter_get_type(tfltr);
	REQUIRE(fltr_type == DUCKDB_TABLE_FILTER_CONJUNCTION_AND);

	// Expect two children for the conjunction-and filter
	idx_t nchildren = duckdb_table_filter_get_children_count(tfltr);
	REQUIRE(nchildren == 2);

	// First child is a constant comparison == 42
	duckdb_table_filter tfltr_child1 = duckdb_table_filter_get_child(tfltr, 0);
	REQUIRE(tfltr_child1 != nullptr);

	duckdb_table_filter_type fltr_child1_type = duckdb_table_filter_get_type(tfltr_child1);
	REQUIRE(fltr_child1_type == DUCKDB_TABLE_FILTER_CONSTANT_COMPARISON);

	duckdb_table_filter_comparison_type fltr_cmp_type = duckdb_table_filter_get_comparison_type(tfltr_child1);
	REQUIRE(fltr_cmp_type == DUCKDB_TABLE_FILTER_COMPARE_EQUAL);

	duckdb_value fltr_const = duckdb_table_filter_get_constant(tfltr_child1);
	REQUIRE(fltr_const != nullptr);

	REQUIRE(duckdb_get_int64(fltr_const) == 42);

	// Second child is a not null filter
	duckdb_table_filter tfltr_child2 = duckdb_table_filter_get_child(tfltr, 1);
	REQUIRE(tfltr_child2 != nullptr);

	duckdb_table_filter_type fltr_child2_type = duckdb_table_filter_get_type(tfltr_child2);
	REQUIRE(fltr_child2_type == DUCKDB_TABLE_FILTER_IS_NOT_NULL);

	// Third child non-existent; expect nullptr
	duckdb_table_filter tfltr_child3 = duckdb_table_filter_get_child(tfltr, 2);
	REQUIRE(tfltr_child3 == nullptr);
}

void fltr_fn(duckdb_function_info info, duckdb_data_chunk output) {
}

TEST_CASE("Test Table Function Filter Pushdown C API", "[capi]") {
	duckdb_database db;
	duckdb_connection con;

	// open a database
	REQUIRE(duckdb_open(nullptr, &db) == DuckDBSuccess);
	REQUIRE(duckdb_connect(db, &con) == DuckDBSuccess);

	// create a table function
	auto function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, "myfn");
	duckdb_table_function_set_bind(function, fltr_bind);
	duckdb_table_function_set_init(function, fltr_init);
	duckdb_table_function_set_function(function, fltr_fn);
	duckdb_table_function_supports_filter_pushdown(function, true);
	duckdb_table_function_supports_filter_prune(function, true);

	// register
	REQUIRE(duckdb_register_table_function(con, function) == DuckDBSuccess);

	// now call it
	duckdb_query(con, "SELECT * FROM myfn() WHERE i = 42", nullptr);

	// cleanup
	duckdb_destroy_table_function(&function);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}

void sfltr_bind(duckdb_bind_info info) {

	duckdb_logical_type mtypes[1] = {duckdb_create_logical_type(DUCKDB_TYPE_BIGINT)};
	const char *mnames[1] = {"i"};
	duckdb_logical_type type = duckdb_create_struct_type(mtypes, mnames, 1);
	duckdb_bind_add_result_column(info, "s", type);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&mtypes[0]);

	duckdb_bind_set_bind_data(info, nullptr, free);
}

void sfltr_init(duckdb_init_info info) {
	duckdb_table_filters filters = duckdb_init_get_table_filters(info);
	REQUIRE(filters != nullptr);
	// TODO: test case where there are no filters

	// Expect one filter
	idx_t nfilters = duckdb_table_filters_size(filters);
	REQUIRE(nfilters == 1);

	// Get the filter
	duckdb_table_filter tfltr = duckdb_table_filters_get_filter(filters, 0);
	REQUIRE(tfltr != nullptr);

	// Expect a conjunction-and filter
	duckdb_table_filter_type fltr_type = duckdb_table_filter_get_type(tfltr);
	REQUIRE(fltr_type == DUCKDB_TABLE_FILTER_CONJUNCTION_AND);

	// Expect two children for the conjunction-and filter
	idx_t nchildren = duckdb_table_filter_get_children_count(tfltr);
	REQUIRE(nchildren == 2);

	// First child is a struct extract filter with constant compare i == 42
	duckdb_table_filter tfltr_child1 = duckdb_table_filter_get_child(tfltr, 0);
	REQUIRE(tfltr_child1 != nullptr);

	duckdb_table_filter_type fltr_child1_type = duckdb_table_filter_get_type(tfltr_child1);
	REQUIRE(fltr_child1_type == DUCKDB_TABLE_FILTER_STRUCT_EXTRACT);

	idx_t child1_idx = duckdb_table_filter_get_struct_child_index(tfltr_child1);
	REQUIRE(child1_idx == 0);

	const char *child1_name = duckdb_table_filter_get_struct_child_name(tfltr_child1);
	REQUIRE(strcmp(child1_name, "i") == 0);

	duckdb_table_filter c1_child = duckdb_table_filter_get_struct_child_filter(tfltr_child1);
	REQUIRE(c1_child != nullptr);

	duckdb_table_filter_type c1_child_type = duckdb_table_filter_get_type(c1_child);
	REQUIRE(c1_child_type == DUCKDB_TABLE_FILTER_CONSTANT_COMPARISON);

	duckdb_table_filter_comparison_type c1_cmp_type = duckdb_table_filter_get_comparison_type(c1_child);
	REQUIRE(c1_cmp_type == DUCKDB_TABLE_FILTER_COMPARE_EQUAL);

	duckdb_value c1_const = duckdb_table_filter_get_constant(c1_child);
	REQUIRE(c1_const != nullptr);

	REQUIRE(duckdb_get_int64(c1_const) == 42);

	// Second child is a struct extract filter with is not null
	duckdb_table_filter tfltr_child2 = duckdb_table_filter_get_child(tfltr, 1);
	REQUIRE(tfltr_child2 != nullptr);

	duckdb_table_filter_type fltr_child2_type = duckdb_table_filter_get_type(tfltr_child2);
	REQUIRE(fltr_child2_type == DUCKDB_TABLE_FILTER_STRUCT_EXTRACT);

	idx_t child2_idx = duckdb_table_filter_get_struct_child_index(tfltr_child2);
	REQUIRE(child2_idx == 0);

	const char *child2_name = duckdb_table_filter_get_struct_child_name(tfltr_child2);
	REQUIRE(strcmp(child2_name, "i") == 0);

	duckdb_table_filter c2_child = duckdb_table_filter_get_struct_child_filter(tfltr_child2);
	REQUIRE(c2_child != nullptr);

	duckdb_table_filter_type c2_child_type = duckdb_table_filter_get_type(c2_child);
	REQUIRE(c2_child_type == DUCKDB_TABLE_FILTER_IS_NOT_NULL);
}

void sfltr_fn(duckdb_function_info info, duckdb_data_chunk output) {
}

TEST_CASE("Test Table Function Struct Filter Pushdown C API", "[capi]") {
	duckdb_database db;
	duckdb_connection con;

	// open a database
	REQUIRE(duckdb_open(nullptr, &db) == DuckDBSuccess);
	REQUIRE(duckdb_connect(db, &con) == DuckDBSuccess);

	// create a table function
	auto function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, "myfn");
	duckdb_table_function_set_bind(function, sfltr_bind);
	duckdb_table_function_set_init(function, sfltr_init);
	duckdb_table_function_set_function(function, sfltr_fn);
	duckdb_table_function_supports_filter_pushdown(function, true);
	duckdb_table_function_supports_filter_prune(function, true);

	// register
	REQUIRE(duckdb_register_table_function(con, function) == DuckDBSuccess);

	// now call it
	duckdb_query(con, "SELECT * FROM myfn() WHERE s.i = 42", nullptr);

	// cleanup
	duckdb_destroy_table_function(&function);
	duckdb_disconnect(&con);
	duckdb_close(&db);
}
