#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void first_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::SetFirst(inputs[0], result);
}

AggregateFunction First::GetFunction() {
	return AggregateFunction("first", get_same_return_type, get_return_type_size, null_state_initialize, first_update, gather_finalize);
}

} // namespace duckdb
