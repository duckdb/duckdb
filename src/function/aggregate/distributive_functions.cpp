#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void gather_finalize(Vector &states, Vector &result) {
	VectorOperations::Gather::Set(states, result);
}

void null_state_initialize(data_ptr_t state, TypeId return_type) {
	SetNullValue(state, return_type);
}

index_t get_bigint_type_size(TypeId return_type) {
	return GetTypeIdSize(TypeId::BIGINT);
}

void bigint_payload_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, get_bigint_type_size(return_type));
}

Value bigint_simple_initialize() {
	return Value::BIGINT(0);
}

SQLType get_bigint_return_type(vector<SQLType> &arguments) {
	if (arguments.size() > 1)
		return SQLTypeId::INVALID;
	return SQLTypeId::BIGINT;
}

index_t get_return_type_size(TypeId return_type) {
	return GetTypeIdSize(return_type);
}


Value null_simple_initialize() {
	return Value();
}

SQLType get_same_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 1)
		return SQLTypeId::INVALID;
	return arguments[0];
}

void BuiltinFunctions::RegisterDistributiveAggregates() {
	AddFunction(AggregateFunction("count_star", get_bigint_return_type, get_bigint_type_size, bigint_payload_initialize, countstar_update, gather_finalize, bigint_simple_initialize, countstar_simple_update, false));
	AddFunction(AggregateFunction("count", get_bigint_return_type, get_bigint_type_size, bigint_payload_initialize, count_update, gather_finalize, bigint_simple_initialize, count_simple_update, false));

	AddFunction(AggregateFunction("first", get_same_return_type, get_return_type_size, null_state_initialize, first_update, gather_finalize));
	AddFunction(AggregateFunction("max", get_same_return_type, get_return_type_size, null_state_initialize, max_update, gather_finalize, null_simple_initialize, max_simple_update));
	AddFunction(AggregateFunction("min", get_same_return_type, get_return_type_size, null_state_initialize, min_update, gather_finalize, null_simple_initialize, min_simple_update));
	AddFunction(AggregateFunction("sum", sum_get_return_type, get_return_type_size, null_state_initialize, sum_update, gather_finalize, null_simple_initialize, sum_simple_update));
	AddFunction(AggregateFunction("string_agg", string_agg_get_return_type, get_return_type_size, null_state_initialize, string_agg_update, gather_finalize));

}

} // namespace duckdb

