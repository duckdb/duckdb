#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void min_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Min(inputs[0], result);
}

void min_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value min = VectorOperations::Min(inputs[0]);
	if (min.is_null) {
		return;
	}
	if (result.is_null || result > min) {
		result = min;
	}
}

AggregateFunction Min::GetFunction() {
	return AggregateFunction("min", get_same_return_type, get_return_type_size, null_state_initialize, min_update, gather_finalize, null_simple_initialize, min_simple_update);
}

} // namespace duckdb
