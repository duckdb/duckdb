#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

void countstar_update(Vector inputs[], index_t input_count, Vector &result) {
	// add one to each address, regardless of if the value is NULL
	Vector one(Value::BIGINT(1));
	VectorOperations::Scatter::Add(one, result);
}

void countstar_simple_update(Vector inputs[], index_t input_count, Value &result) {
	assert(input_count == 1);
	Value count = Value::BIGINT(inputs[0].count);
	result = result + count;
}


AggregateFunction CountStar::GetFunction() {
	return AggregateFunction("count_star", get_bigint_return_type, get_bigint_type_size, bigint_payload_initialize, countstar_update, gather_finalize, bigint_simple_initialize, countstar_simple_update, false);
}

} // namespace duckdb
