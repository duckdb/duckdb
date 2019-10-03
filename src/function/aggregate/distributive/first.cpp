#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;
using namespace duckdb;

static void first_update(Vector inputs[], index_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::SetFirst(inputs[0], result);
}

namespace duckdb {

AggregateFunction First::GetFunction(SQLType type) {
	return AggregateFunction({ type }, type, get_return_type_size, null_state_initialize, first_update, nullptr, gather_finalize);
}

void First::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet first("first");
	for(auto type : SQLType::ALL_TYPES) {
		first.AddFunction(First::GetFunction(type));
	}
	set.AddFunction(first);
}

} // namespace duckdb
