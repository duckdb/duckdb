#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

template <class T, class T2>
struct ArgMinMaxState {
	T arg;
	T2 value;
	bool is_initialized;
};

template <class T>
static void ArgMinMaxDestroyValue(T value) {
}

template <>
void ArgMinMaxDestroyValue(string_t value) {
	if (!value.IsInlined()) {
		delete[] value.GetDataUnsafe();
	}
}

template <class T>
static void ArgMinMaxAssignValue(T &target, T new_value, bool is_initialized) {
	target = new_value;
}

template <>
void ArgMinMaxAssignValue(string_t &target, string_t new_value, bool is_initialized) {
	if (is_initialized) {
		ArgMinMaxDestroyValue(target);
	}
	if (new_value.IsInlined()) {
		target = new_value;
	} else {
		// non-inlined string, need to allocate space for it
		auto len = new_value.GetSize();
		auto ptr = new char[len];
		memcpy(ptr, new_value.GetDataUnsafe(), len);

		target = string_t(ptr, len);
	}
}

template <class COMPARATOR>
struct ArgMinMaxBase {
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->is_initialized) {
			ArgMinMaxDestroyValue(state->arg);
			ArgMinMaxDestroyValue(state->value);
		}
	}

	template <class STATE>
	static void Initialize(STATE *state) {
		state->is_initialized = false;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, ValidityMask &amask,
	                      ValidityMask &bmask, idx_t xidx, idx_t yidx) {
		if (!state->is_initialized) {
			ArgMinMaxAssignValue<A_TYPE>(state->arg, x_data[xidx], false);
			ArgMinMaxAssignValue<B_TYPE>(state->value, y_data[yidx], false);
			state->is_initialized = true;
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x_data[xidx], y_data[yidx]);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE *state, A_TYPE x_data, B_TYPE y_data) {
		if (COMPARATOR::Operation(y_data, state->value)) {
			ArgMinMaxAssignValue<A_TYPE>(state->arg, x_data, true);
			ArgMinMaxAssignValue<B_TYPE>(state->value, y_data, true);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, FunctionData *bind_data) {
		if (!source.is_initialized) {
			return;
		}
		if (!target->is_initialized || COMPARATOR::Operation(source.value, target->value)) {
			ArgMinMaxAssignValue(target->arg, source.arg, target->is_initialized);
			ArgMinMaxAssignValue(target->value, source.value, target->is_initialized);
			target->is_initialized = true;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class COMPARATOR>
struct StringArgMinMax : public ArgMinMaxBase<COMPARATOR> {
	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_initialized) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->arg);
		}
	}
};

template <class COMPARATOR>
struct NumericArgMinMax : public ArgMinMaxBase<COMPARATOR> {
	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_initialized) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->arg;
		}
	}
};

using NumericArgMinOperation = NumericArgMinMax<LessThan>;
using NumericArgMaxOperation = NumericArgMinMax<GreaterThan>;
using StringArgMinOperation = StringArgMinMax<LessThan>;
using StringArgMaxOperation = StringArgMinMax<GreaterThan>;

template <class OP, class T, class T2>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &arg_2, const LogicalType &arg) {
	auto function = AggregateFunction::BinaryAggregate<ArgMinMaxState<T, T2>, T, T2, T, OP>(arg, arg_2, arg);
	if (arg.InternalType() == PhysicalType::VARCHAR || arg_2.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<ArgMinMaxState<T, T2>, OP>;
	}
	return function;
}
template <class OP, class T>
AggregateFunction GetArgMinMaxFunctionArg2(const LogicalType &arg_2, const LogicalType &arg) {
	switch (arg_2.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, T, int32_t>(arg_2, arg);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, T, int64_t>(arg_2, arg);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, T, double>(arg_2, arg);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, T, string_t>(arg_2, arg);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class T>
void AddArgMinMaxFunctionArg2(AggregateFunctionSet &fun, const LogicalType &arg) {
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::INTEGER, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::BIGINT, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::DOUBLE, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::VARCHAR, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::DATE, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::TIMESTAMP, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::TIMESTAMP_TZ, arg));
	fun.AddFunction(GetArgMinMaxFunctionArg2<OP, T>(LogicalType::BLOB, arg));
}

template <class OP, class STRING_OP>
static void AddArgMinMaxFunctions(AggregateFunctionSet &fun) {
	AddArgMinMaxFunctionArg2<OP, int32_t>(fun, LogicalType::INTEGER);
	AddArgMinMaxFunctionArg2<OP, int64_t>(fun, LogicalType::BIGINT);
	AddArgMinMaxFunctionArg2<OP, double>(fun, LogicalType::DOUBLE);
	AddArgMinMaxFunctionArg2<STRING_OP, string_t>(fun, LogicalType::VARCHAR);
	AddArgMinMaxFunctionArg2<OP, date_t>(fun, LogicalType::DATE);
	AddArgMinMaxFunctionArg2<OP, timestamp_t>(fun, LogicalType::TIMESTAMP);
	AddArgMinMaxFunctionArg2<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ);
	AddArgMinMaxFunctionArg2<STRING_OP, string_t>(fun, LogicalType::BLOB);
}

void ArgMinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("argmin");
	AddArgMinMaxFunctions<NumericArgMinOperation, StringArgMinOperation>(fun);
	set.AddFunction(fun);

	//! Add min_by alias
	fun.name = "min_by";
	set.AddFunction(fun);

	//! Add arg_min alias
	fun.name = "arg_min";
	set.AddFunction(fun);
}

void ArgMaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("argmax");
	AddArgMinMaxFunctions<NumericArgMaxOperation, StringArgMaxOperation>(fun);
	set.AddFunction(fun);

	//! Add max_by alias
	fun.name = "max_by";
	set.AddFunction(fun);

	//! Add arg_max alias
	fun.name = "arg_max";
	set.AddFunction(fun);
}

} // namespace duckdb
