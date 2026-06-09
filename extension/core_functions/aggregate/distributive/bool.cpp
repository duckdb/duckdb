#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"

namespace duckdb {

namespace {

using BoolState = optional<bool>;

template <class REDUCE_OP, bool INIT_VALUE>
struct BoolAggregate {
	// No Initialize: StateInitialize falls back to memset(state, 0) = nullopt

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state = REDUCE_OP::template Operation<bool>(state.value_or(INIT_VALUE), bool(input));
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		if (count > 0) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.has_value()) {
			return;
		}
		target = REDUCE_OP::template Operation<bool>(target.value_or(INIT_VALUE), *source);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.has_value()) {
			finalize_data.ReturnNull();
			return;
		}
		target = *state;
	}

	static bool IgnoreNull() {
		return true;
	}
};

using BoolAndFunFunction = BoolAggregate<LogicalAnd, true>;
using BoolOrFunFunction = BoolAggregate<LogicalOr, false>;

} // namespace

AggregateFunction BoolOrFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolOrFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	return fun;
}

AggregateFunction BoolAndFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolAndFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	return fun;
}

} // namespace duckdb
