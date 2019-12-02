#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;
using namespace duckdb;

static void max_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Max(inputs[0], result);
}

static void max_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Max(state, combined);
}

static void max_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value max = VectorOperations::Max(inputs[0]);
	if (max.is_null) {
		return;
	}
	if (result.is_null || result < max) {
		result = max;
	}
}

namespace duckdb {

void MaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet max("max");
	for (auto type : SQLType::ALL_TYPES) {
		max.AddFunction(AggregateFunction({type}, type, get_return_type_size, null_state_initialize, max_update,
		                                  max_combine, gather_finalize, null_simple_initialize, max_simple_update));
	}
	set.AddFunction(max);
}

} // namespace duckdb
