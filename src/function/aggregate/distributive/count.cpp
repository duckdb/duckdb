#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void count_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::AddOne(inputs[0], result);
}

void count_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value count = VectorOperations::Count(inputs[0]);
	result = result + count;
}

AggregateFunction Count::GetFunction() {
	return AggregateFunction("count", get_bigint_return_type, get_bigint_type_size, bigint_payload_initialize, count_update, gather_finalize, bigint_simple_initialize, count_simple_update, false);
}

} // namespace duckdb
