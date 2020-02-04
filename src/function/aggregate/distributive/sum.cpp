#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

using namespace std;

namespace duckdb {

static void sum_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Add(inputs[0], result);
}

static void sum_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Add(state, combined);
}

template <class T> static void sum_simple_update(Vector inputs[], index_t input_count, data_ptr_t state_) {
	auto state = (T *)state_;
	T result;
	if (!AggregateExecutor::Execute<T, T, duckdb::Add>(inputs[0], &result)) {
		// no non-null values encountered
		return;
	}
	if (inputs[0].vector_type == VectorType::CONSTANT_VECTOR) {
		result *= inputs[0].count;
	}
	if (IsNullValue<T>(*state)) {
		*state = result;
	} else {
		*state += result;
	}
}

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// integer sums to bigint
	sum.AddFunction(AggregateFunction({SQLType::BIGINT}, SQLType::BIGINT, get_return_type_size, null_state_initialize,
	                                  sum_update, sum_combine, gather_finalize, sum_simple_update<int64_t>));
	// float sums to float
	sum.AddFunction(AggregateFunction({SQLType::DOUBLE}, SQLType::DOUBLE, get_return_type_size, null_state_initialize,
	                                  sum_update, sum_combine, gather_finalize, sum_simple_update<double>));

	set.AddFunction(sum);
}

} // namespace duckdb
