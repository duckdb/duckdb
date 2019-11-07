#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;
using namespace duckdb;

static void min_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Min(inputs[0], result);
}

static void min_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Min(state, combined);
}

static void min_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value min = VectorOperations::Min(inputs[0]);
	if (min.is_null) {
		return;
	}
	if (result.is_null || result > min) {
		result = min;
	}
}

namespace duckdb {

void MinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet min("min");
	for (auto type : SQLType::ALL_TYPES) {
		min.AddFunction(AggregateFunction({type}, type, get_return_type_size, null_state_initialize, min_update,
		                                  min_combine, gather_finalize, null_simple_initialize, min_simple_update));
	}
	set.AddFunction(min);
}

} // namespace duckdb
