#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void sum_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Add(inputs[0], result);
}

void sum_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value sum = VectorOperations::Sum(inputs[0]);
	if (sum.is_null) {
		return;
	}
	if (result.is_null) {
		result = sum;
	} else {
		result = result + sum;
	}
}

void Sum::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// integer sums to bigint
	sum.AddFunction(AggregateFunction({ SQLType::BIGINT }, SQLType::BIGINT, get_return_type_size, null_state_initialize, sum_update, gather_finalize, null_simple_initialize, sum_simple_update));
	// float sums to float
	sum.AddFunction(AggregateFunction({ SQLType::DOUBLE }, SQLType::DOUBLE, get_return_type_size, null_state_initialize, sum_update, gather_finalize, null_simple_initialize, sum_simple_update));

	set.AddFunction(sum);
}

} // namespace duckdb
