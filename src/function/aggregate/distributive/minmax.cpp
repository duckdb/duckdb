#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"

using namespace std;

namespace duckdb {

static void min_update(Vector inputs[], idx_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Min(inputs[0], result);
}

static void min_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Min(state, combined);
}

static void max_update(Vector inputs[], idx_t input_count, Vector &result) {
	assert(input_count == 1);
	VectorOperations::Scatter::Max(inputs[0], result);
}

static void max_combine(Vector &state, Vector &combined) {
	VectorOperations::Scatter::Max(state, combined);
}

template <class T, class OP> static void minmax_simple_update(Vector inputs[], idx_t input_count, data_ptr_t state_) {
	auto state = (T *)state_;
	T result;
	if (!AggregateExecutor::Execute<T, T, OP>(inputs[0], &result)) {
		// no non-null values encountered
		return;
	}
	if (IsNullValue<T>(*state)) {
		*state = result;
	} else {
		*state = OP::Operation(*state, result);
	}
}

template <class OP> static aggregate_simple_update_t MinMaxFunction(SQLType sql_type) {
	auto type = GetInternalType(sql_type);
	switch (type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return minmax_simple_update<int8_t, OP>;
	case TypeId::INT16:
		return minmax_simple_update<int16_t, OP>;
	case TypeId::INT32:
		return minmax_simple_update<int32_t, OP>;
	case TypeId::INT64:
		return minmax_simple_update<int64_t, OP>;
	case TypeId::FLOAT:
		return minmax_simple_update<float, OP>;
	case TypeId::DOUBLE:
		return minmax_simple_update<double, OP>;
	case TypeId::VARCHAR:
		return minmax_simple_update<string_t, OP>;
	default:
		// TODO LIST/STRUCT
		throw NotImplementedException("FIXME: unimplemented type for MIN/MAX");
	}
}

void MinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet min("min");
	for (auto type : SQLType::ALL_TYPES) {
		min.AddFunction(AggregateFunction({type}, type, get_return_type_size, null_state_initialize, min_update,
		                                  min_combine, gather_finalize, MinMaxFunction<duckdb::Min>(type)));
	}
	set.AddFunction(min);
}

void MaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet max("max");
	for (auto type : SQLType::ALL_TYPES) {
		max.AddFunction(AggregateFunction({type}, type, get_return_type_size, null_state_initialize, max_update,
		                                  max_combine, gather_finalize, MinMaxFunction<duckdb::Max>(type)));
	}
	set.AddFunction(max);
}

} // namespace duckdb
