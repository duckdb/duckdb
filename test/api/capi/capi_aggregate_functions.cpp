#include "capi_tester.hpp"

using namespace duckdb;
using namespace std;

struct WeightedSumState {
	int64_t sum;
	uint64_t count;
};

idx_t WeightedSumSize() {
	return sizeof(WeightedSumState);
}

void WeightedSumInit(duckdb_aggregate_state state_p) {
	auto state = reinterpret_cast<WeightedSumState *>(state_p);
	state->sum = 0;
	state->count = 0;
}

void WeightedSumUpdate(duckdb_function_info info, duckdb_data_chunk input, duckdb_aggregate_state *states) {
	auto state = reinterpret_cast<WeightedSumState **>(states);
	auto row_count = duckdb_data_chunk_get_size(input);
	auto input_vector = duckdb_data_chunk_get_vector(input, 0);
	auto input_data = static_cast<int64_t *>(duckdb_vector_get_data(input_vector));
	auto input_validity = duckdb_vector_get_validity(input_vector);

	auto weight_vector = duckdb_data_chunk_get_vector(input, 1);
	auto weight_data = static_cast<int64_t *>(duckdb_vector_get_data(weight_vector));
	auto weight_validity = duckdb_vector_get_validity(weight_vector);
	for (idx_t i = 0; i < row_count; i++) {
		if (duckdb_validity_row_is_valid(input_validity, i) && duckdb_validity_row_is_valid(weight_validity, i)) {
			state[i]->sum += input_data[i] * weight_data[i];
			state[i]->count++;
		}
	}
}

void WeightedSumCombine(duckdb_function_info info, duckdb_aggregate_state *source_p, duckdb_aggregate_state *target_p,
                        idx_t count) {
	auto source = reinterpret_cast<WeightedSumState **>(source_p);
	auto target = reinterpret_cast<WeightedSumState **>(target_p);

	for (idx_t i = 0; i < count; i++) {
		target[i]->sum += source[i]->sum;
		target[i]->count += source[i]->count;
	}
}

void WeightedSumFinalize(duckdb_function_info info, duckdb_aggregate_state *source_p, duckdb_vector result, idx_t count,
                         idx_t offset) {
	auto source = reinterpret_cast<WeightedSumState **>(source_p);
	auto result_data = static_cast<int64_t *>(duckdb_vector_get_data(result));
	duckdb_vector_ensure_validity_writable(result);
	auto result_validity = duckdb_vector_get_validity(result);

	for (idx_t i = 0; i < count; i++) {
		if (source[i]->count == 0) {
			duckdb_validity_set_row_invalid(result_validity, offset + i);
		} else {
			result_data[offset + i] = source[i]->sum;
		}
	}
}

static void CAPIRegisterWeightedSum(duckdb_connection connection, const char *name, duckdb_state expected_outcome) {
	duckdb_state status;

	// create a scalar function
	auto function = duckdb_create_aggregate_function();
	duckdb_aggregate_function_set_name(nullptr, name);
	duckdb_aggregate_function_set_name(function, nullptr);
	duckdb_aggregate_function_set_name(function, name);
	duckdb_aggregate_function_set_name(function, name);

	// add a two double parameters
	auto type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_aggregate_function_add_parameter(nullptr, type);
	duckdb_aggregate_function_add_parameter(function, nullptr);
	duckdb_aggregate_function_add_parameter(function, type);
	duckdb_aggregate_function_add_parameter(function, type);

	// set the return type to double
	duckdb_aggregate_function_set_return_type(nullptr, type);
	duckdb_aggregate_function_set_return_type(function, nullptr);
	duckdb_aggregate_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_aggregate_function_set_functions(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
	duckdb_aggregate_function_set_functions(function, nullptr, nullptr, nullptr, nullptr, nullptr);
	duckdb_aggregate_function_set_functions(function, WeightedSumSize, WeightedSumInit, WeightedSumUpdate,
	                                        WeightedSumCombine, WeightedSumFinalize);

	// register and cleanup
	status = duckdb_register_aggregate_function(connection, function);
	REQUIRE(status == expected_outcome);

	duckdb_destroy_aggregate_function(&function);
	duckdb_destroy_aggregate_function(&function);
	duckdb_destroy_aggregate_function(nullptr);
}

TEST_CASE("Test Aggregate Functions C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	CAPIRegisterWeightedSum(tester.connection, "my_weighted_sum", DuckDBSuccess);
	// try to register it again - this should be an error
	CAPIRegisterWeightedSum(tester.connection, "my_weighted_sum", DuckDBError);

	// now call it
	result = tester.Query("SELECT my_weighted_sum(40, 2)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 80);

	result = tester.Query("SELECT my_weighted_sum(40, NULL)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));

	result = tester.Query("SELECT my_weighted_sum(NULL, 2)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->IsNull(0, 0));

	result = tester.Query("SELECT my_weighted_sum(i, 2) FROM range(100) t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->Fetch<int64_t>(0, 0) == 9900);
}
