#include "function/aggregate_function/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void max_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Max(inputs[0], result);
}

void max_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value max = VectorOperations::Max(inputs[0]);
	if (max.is_null) {
		return;
	}
	if (result.is_null || result < max) {
		result = max;
	}
}

} // namespace duckdb
