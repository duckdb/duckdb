#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;
using namespace duckdb;

static void sum_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Add(inputs[0], result);
}

static void sum_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Add(state, combined);
}

static void sum_simple_update(Vector inputs[], index_t input_count, Value &result) {
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

namespace duckdb {

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// integer sums to bigint
	sum.AddFunction(AggregateFunction({SQLType::BIGINT}, SQLType::BIGINT, get_return_type_size, null_state_initialize,
	                                  sum_update, sum_combine, gather_finalize, null_simple_initialize,
	                                  sum_simple_update));
	// float sums to float
	sum.AddFunction(AggregateFunction({SQLType::DOUBLE}, SQLType::DOUBLE, get_return_type_size, null_state_initialize,
	                                  sum_update, sum_combine, gather_finalize, null_simple_initialize,
	                                  sum_simple_update));

	set.AddFunction(sum);
}

} // namespace duckdb
