#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void first_update(Vector inputs[], idx_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::SetFirst(inputs[0], result);
}

AggregateFunction FirstFun::GetFunction(SQLType type) {
	return AggregateFunction({type}, type, get_return_type_size, null_state_initialize, first_update, nullptr,
	                         gather_finalize);
}

void FirstFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet first("first");
	for (auto type : SQLType::ALL_TYPES) {
		first.AddFunction(FirstFun::GetFunction(type));
	}
	set.AddFunction(first);
}

} // namespace duckdb
