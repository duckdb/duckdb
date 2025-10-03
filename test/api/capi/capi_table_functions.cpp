#include "capi_tester.hpp"

#include <vector>

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

	// registering again does not cause error, because we overload
	capi_register_table_function(tester.connection, "my_function", my_bind, my_init, my_function);

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
	// Try to register it again with the same name, is ok (because of overloading)
	capi_register_table_function(tester.connection, "x", my_error_bind, my_init, my_function, DuckDBSuccess);
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

struct my_bind_connection_id_data {
	idx_t connection_id;
	idx_t rows_requested;
};

void my_bind_connection_id(duckdb_bind_info info) {
	REQUIRE(duckdb_bind_get_parameter_count(info) == 1);

	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "connection_id", type);
	duckdb_destroy_logical_type(&type);

	type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "forty_two", type);
	duckdb_destroy_logical_type(&type);

	auto bind_data = (my_bind_connection_id_data *)malloc(sizeof(my_bind_connection_id_data));
	auto param = duckdb_bind_get_parameter(info, 0);
	auto rows_requested = duckdb_get_int64(param);
	duckdb_destroy_value(&param);

	duckdb_client_context context;
	duckdb_table_function_get_client_context(info, &context);
	auto connection_id = duckdb_client_context_get_connection_id(context);
	duckdb_destroy_client_context(&context);

	bind_data->rows_requested = rows_requested;
	bind_data->connection_id = connection_id;
	duckdb_bind_set_bind_data(info, bind_data, free);
}

void my_init_connection_id(duckdb_init_info info) {
	REQUIRE(duckdb_init_get_bind_data(info) != nullptr);
	REQUIRE(duckdb_init_get_bind_data(nullptr) == nullptr);

	auto init_data = (my_init_data_struct *)malloc(sizeof(my_init_data_struct));
	init_data->pos = 0;
	duckdb_init_set_init_data(info, init_data, free);
}

void my_function_connection_id(duckdb_function_info info, duckdb_data_chunk output) {
	auto bind_data = (my_bind_connection_id_data *)duckdb_function_get_bind_data(info);
	auto init_data = (my_init_data_struct *)duckdb_function_get_init_data(info);
	auto ptr = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
	auto ptr2 = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
	idx_t i;
	for (i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (init_data->pos >= bind_data->rows_requested) {
			break;
		}
		ptr[i] = bind_data->connection_id;
		ptr2[i] = 42;
		init_data->pos++;
	}
	duckdb_data_chunk_set_size(output, i);
}

TEST_CASE("Table function client context return") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	capi_register_table_function(tester.connection, "my_connection_id_function", my_bind_connection_id,
	                             my_init_connection_id, my_function_connection_id);

	duckdb_client_context context;
	duckdb_connection_get_client_context(tester.connection, &context);
	auto first_conn_id = duckdb_client_context_get_connection_id(context);
	duckdb_destroy_client_context(&context);

	result = tester.Query("SELECT * FROM my_connection_id_function(3)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == first_conn_id);
	REQUIRE(result->Fetch<int64_t>(0, 1) == first_conn_id);
	REQUIRE(result->Fetch<int64_t>(0, 2) == first_conn_id);
	REQUIRE(result->Fetch<int64_t>(1, 0) == 42);
	REQUIRE(result->Fetch<int64_t>(1, 1) == 42);
	REQUIRE(result->Fetch<int64_t>(1, 2) == 42);
}

struct capi_filter_node {
	duckdb_table_filter_type type = DUCKDB_TABLE_FILTER_TYPE_INVALID;
	idx_t column_index = 0;
	duckdb_table_filter_operator op = DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	bool has_operator = false;
	int64_t constant = 0;
	bool has_constant = false;
	std::vector<capi_filter_node> children;
};

static std::vector<capi_filter_node> capi_last_filters;
static idx_t capi_last_filter_count = 0;

static capi_filter_node CaptureFilterNode(duckdb_table_function_filter filter) {
	capi_filter_node node;
	node.type = duckdb_table_function_filter_get_type(filter);
	node.column_index = duckdb_table_function_filter_get_column_index(filter);
	if (node.type == DUCKDB_TABLE_FILTER_TYPE_CONSTANT_COMPARISON) {
		node.has_operator = true;
		node.op = duckdb_table_function_filter_get_operator(filter);
		duckdb_value constant = duckdb_table_function_filter_get_constant(filter);
		REQUIRE(constant);
		node.has_constant = true;
		node.constant = duckdb_get_int64(constant);
		duckdb_destroy_value(&constant);
	} else {
		node.has_operator = false;
		node.op = DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
		node.has_constant = false;
	}
	auto child_count = duckdb_table_function_filter_get_child_count(filter);
	for (idx_t i = 0; i < child_count; i++) {
		duckdb_table_function_filter child;
		REQUIRE(duckdb_table_function_filter_get_child(filter, i, &child) == DuckDBSuccess);
		REQUIRE(child);
		node.children.push_back(CaptureFilterNode(child));
		duckdb_destroy_table_function_filter(&child);
	}
	return node;
}

struct capi_expected_filter_node {
	duckdb_table_filter_type type;
	idx_t column_index;
	duckdb_table_filter_operator op;
	bool has_operator;
	int64_t constant;
	bool has_constant;
	std::vector<capi_expected_filter_node> children;
};

static capi_expected_filter_node MakeConstantNode(duckdb_table_filter_operator op, int64_t constant) {
	capi_expected_filter_node node;
	node.type = DUCKDB_TABLE_FILTER_TYPE_CONSTANT_COMPARISON;
	node.column_index = 0;
	node.op = op;
	node.has_operator = true;
	node.constant = constant;
	node.has_constant = true;
	return node;
}

static capi_expected_filter_node MakeConjunctionNode(duckdb_table_filter_type type,
                                                     std::vector<capi_expected_filter_node> children) {
	capi_expected_filter_node node;
	node.type = type;
	node.column_index = 0;
	node.op = DUCKDB_TABLE_FILTER_OPERATOR_INVALID;
	node.has_operator = false;
	node.constant = 0;
	node.has_constant = false;
	node.children = std::move(children);
	return node;
}

static bool FilterNodeMatches(const capi_filter_node &captured, const capi_expected_filter_node &expected) {
	if (captured.type != expected.type) {
		return false;
	}
	if (captured.column_index != expected.column_index) {
		return false;
	}
	if (captured.has_operator != expected.has_operator) {
		return false;
	}
	if (expected.has_operator && captured.op != expected.op) {
		return false;
	}
	if (captured.has_constant != expected.has_constant) {
		return false;
	}
	if (expected.has_constant && captured.constant != expected.constant) {
		return false;
	}
	if (captured.children.size() != expected.children.size()) {
		return false;
	}
	std::vector<bool> matched(expected.children.size(), false);
	for (auto &child : captured.children) {
		bool found = false;
		for (idx_t i = 0; i < expected.children.size(); i++) {
			if (matched[i]) {
				continue;
			}
			if (FilterNodeMatches(child, expected.children[i])) {
				matched[i] = true;
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

void capi_filter_pushdown_bind(duckdb_bind_info info) {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_bind_add_result_column(info, "value", type);
	duckdb_destroy_logical_type(&type);
}

void capi_filter_pushdown_init(duckdb_init_info info) {
	capi_last_filters.clear();
	capi_last_filter_count = duckdb_init_get_filter_count(info);
	for (idx_t i = 0; i < capi_last_filter_count; i++) {
		duckdb_table_function_filter filter;
		REQUIRE(duckdb_init_get_filter(info, i, &filter) == DuckDBSuccess);
		REQUIRE(filter);

		capi_last_filters.push_back(CaptureFilterNode(filter));
		duckdb_destroy_table_function_filter(&filter);
	}
}

void capi_filter_pushdown_function(duckdb_function_info info, duckdb_data_chunk output) {
	duckdb_data_chunk_set_size(output, 0);
}

static void capi_register_filter_pushdown_function(duckdb_connection connection, const char *name) {
	auto function = duckdb_create_table_function();
	duckdb_table_function_set_name(function, name);
	duckdb_table_function_set_bind(function, capi_filter_pushdown_bind);
	duckdb_table_function_set_init(function, capi_filter_pushdown_init);
	duckdb_table_function_set_function(function, capi_filter_pushdown_function);
	duckdb_table_function_supports_projection_pushdown(function, true);
	duckdb_table_function_supports_filter_pushdown(function, true);
	REQUIRE(duckdb_register_table_function(connection, function) == DuckDBSuccess);
	duckdb_destroy_table_function(&function);
}

TEST_CASE("Table function filter pushdown via C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	capi_register_filter_pushdown_function(tester.connection, "capi_filter_pushdown");

	struct QueryExpectation {
		const char *query;
		std::vector<capi_expected_filter_node> expected_filters;
	};

	std::vector<QueryExpectation> expectations = {
	    {"SELECT * FROM capi_filter_pushdown() WHERE value = 5",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_EQUAL, 5)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value != 3",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_NOT_EQUAL, 3)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value > 7",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_GREATER_THAN, 7)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value >= 8",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_GREATER_THAN_OR_EQUAL, 8)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value < 4",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_LESS_THAN, 4)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value <= 6",
	     {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_LESS_THAN_OR_EQUAL, 6)}},
	    {"SELECT * FROM capi_filter_pushdown() WHERE value BETWEEN 2 AND 6",
	     {MakeConjunctionNode(DUCKDB_TABLE_FILTER_TYPE_CONJUNCTION_AND,
	                          {MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_GREATER_THAN_OR_EQUAL, 2),
	                           MakeConstantNode(DUCKDB_TABLE_FILTER_OPERATOR_LESS_THAN_OR_EQUAL, 6)})}},
	    // No filters for this case, since the optimizer doesn't attempt to push OR clauses into scans
	    {"SELECT * FROM capi_filter_pushdown() WHERE value = 5 OR value = 7", {}}};

	for (auto &expectation : expectations) {
		capi_last_filters.clear();
		capi_last_filter_count = 0;

		result = tester.Query(expectation.query);
		REQUIRE_NO_FAIL(*result);

		REQUIRE(capi_last_filter_count == expectation.expected_filters.size());
		REQUIRE(capi_last_filters.size() == expectation.expected_filters.size());

		std::vector<bool> matched(capi_last_filters.size(), false);
		for (auto &expected : expectation.expected_filters) {
			bool found_match = false;
			for (idx_t i = 0; i < capi_last_filters.size(); i++) {
				if (matched[i]) {
					continue;
				}
				if (FilterNodeMatches(capi_last_filters[i], expected)) {
					matched[i] = true;
					found_match = true;
					break;
				}
			}
			REQUIRE(found_match);
		}
	}
}
