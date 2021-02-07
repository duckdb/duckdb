#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include <utility>

namespace duckdb {
template <class T, class T2>
struct arg_min_max_state_t {
	T arg;
	T2 value;
	bool is_initialized;
};

struct ArgMinMaxOperation {

	template <class STATE>
	static void Initialize(STATE *state) {
		state->is_initialized = false;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->is_initialized) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->arg;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ArgMinOperation : ArgMinMaxOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		if (!state->is_initialized) {
			state->value = y_data[yidx];
			state->arg = x_data[xidx];
			state->is_initialized = true;
		} else {
			if (y_data[yidx] < state->value) {
				state->value = y_data[yidx];
				state->arg = x_data[xidx];
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(STATE &source, STATE *target) {
		if (!source.is_initialized) {
			return;
		}
		if (!target->is_initialized) {
			target->is_initialized = true;
			target->value = source.value;
			target->arg = source.arg;
			return;
		}
		if (source.value < target->value) {
			target->value = source.value;
			target->arg = source.arg;
		}
	}
};

struct ArgMaxOperation : ArgMinMaxOperation {

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask,
	                      nullmask_t &bnullmask, idx_t xidx, idx_t yidx) {
		if (!state->is_initialized) {
			state->value = y_data[yidx];
			state->arg = x_data[xidx];
			state->is_initialized = true;
		} else {
			if (y_data[yidx] > state->value) {
				state->value = y_data[yidx];
				state->arg = x_data[xidx];
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(STATE &source, STATE *target) {
		if (!source.is_initialized) {
			return;
		}
		if (!target->is_initialized) {
			target->is_initialized = true;
			target->value = source.value;
			target->arg = source.arg;
			return;
		}
		if (source.value > target->value) {
			target->value = source.value;
			target->arg = source.arg;
		}
	}
};

void ArgMinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("arg_min");
	fun.AddFunction(AggregateFunction::BinaryAggregate<arg_min_max_state_t<double, double>, double, double, double,
	                                                   ArgMinOperation>(LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                                                    LogicalType::DOUBLE));

	fun.AddFunction(AggregateFunction::BinaryAggregate<arg_min_max_state_t<string_t, double>, string_t, double,
	                                                   string_t, ArgMinOperation>(
	    LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::VARCHAR));
	set.AddFunction(fun);
}

void ArgMaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("arg_max");
	fun.AddFunction(AggregateFunction::BinaryAggregate<arg_min_max_state_t<double, double>, double, double, double,
	                                                   ArgMaxOperation>(LogicalType::DOUBLE, LogicalType::DOUBLE,
	                                                                    LogicalType::DOUBLE));
	fun.AddFunction(AggregateFunction::BinaryAggregate<arg_min_max_state_t<string_t, double>, string_t, double,
	                                                   string_t, ArgMaxOperation>(
	    LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::VARCHAR));
	set.AddFunction(fun);
}

} // namespace duckdb