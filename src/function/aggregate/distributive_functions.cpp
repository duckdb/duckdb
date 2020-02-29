#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void gather_finalize(Vector &states, Vector &result) {
	VectorOperations::Gather::Set(states, result);
}

void null_state_initialize(data_ptr_t state, TypeId return_type) {
	SetNullValue(state, return_type);
}

idx_t get_bigint_type_size(TypeId return_type) {
	return GetTypeIdSize(TypeId::INT64);
}

void bigint_payload_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, get_bigint_type_size(return_type));
}

Value bigint_simple_initialize() {
	return Value::BIGINT(0);
}
idx_t get_return_type_size(TypeId return_type) {
	return GetTypeIdSize(return_type);
}

Value null_simple_initialize() {
	return Value();
}

void BuiltinFunctions::RegisterDistributiveAggregates() {
	Register<CountStarFun>();
	Register<CountFun>();
	Register<FirstFun>();
	Register<MaxFun>();
	Register<MinFun>();
	Register<SumFun>();
	Register<StringAggFun>();
}

} // namespace duckdb
