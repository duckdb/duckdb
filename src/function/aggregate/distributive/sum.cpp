#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/planner/expression.hpp"

using namespace std;

namespace duckdb {

template<class T>
struct sum_state_t {
	T value;
	bool isset;
};

using hugeint_sum_state_t = sum_state_t<hugeint_t>;

struct numeric_sum_state_t {
	double value;
	bool isset;
};

struct BaseSumOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->value = 0;
		state->isset = false;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			*target = source;
		} else {
			// else perform the operation
			target->value += source.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct IntegerSumOperation : public BaseSumOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->isset = true;
		state->value += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->isset = true;
		state->value += *input * count;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			target[idx] = Hugeint::Convert(state->value);
		}
	}
};

struct SumToHugeintOperation : public BaseSumOperation {
	static void AddValue(hugeint_sum_state_t *state, uint64_t value, int positive) {
		// integer summation taken from Tim Gubner et al. - Efficient Query Processing
		// with Optimistically Compressed Hash Tables & Strings in the USSR

		// add the value to the lower part of the hugeint
		state->value.lower += value;
		// now handle overflows
		int overflow = state->value.lower < value;
		// we consider two situations:
		// (1) input[idx] is positive, and current value is lower than value: overflow
		// (2) input[idx] is negative, and current value is higher than value: underflow
		if (!(overflow ^ positive)) {
			// in the case of an overflow or underflow we either increment or decrement the upper base
			// positive: +1, negative: -1
			state->value.upper += -1 + 2 * positive;
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->isset = true;
		AddValue(state, (uint64_t)input[idx], input[idx] >= 0);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->isset = true;
		// add a constant X number of times
		// fast path: check if value * count fits into a uint64_t
		// note that we check if value * VECTOR_SIZE fits in a uint64_t to avoid having to actually do a division
		// this is still a pretty high number (18014398509481984) so most positive numbers will fit
		if (*input >= 0 && ((uint64_t)*input) < (NumericLimits<uint64_t>::Maximum() / STANDARD_VECTOR_SIZE)) {
			// if it does just multiply it and add the value
			uint64_t value = ((uint64_t)*input) * count;
			AddValue(state, value, 1);
		} else {
			// if it doesn't fit we have two choices
			// either we loop over count and add the values individually
			// or we convert to a hugeint and multiply the hugeint
			// the problem is that hugeint multiplication is expensive
			// hence we switch here: with a low count we do the loop
			// with a high count we do the hugeint multiplication
			if (count < 8) {
				for (idx_t i = 0; i < count; i++) {
					AddValue(state, (uint64_t)*input, *input >= 0);
				}
			} else {
				hugeint_t addition = hugeint_t(*input) * count;
				state->value += addition;
			}
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->value;
		}
	}
};

struct NumericSumOperation : public BaseSumOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->isset = true;
		state->value += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->isset = true;
		state->value += double(input[0]) * double(count);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			if (!Value::DoubleIsValid(state->value)) {
				throw OutOfRangeException("SUM is out of range!");
			}
			target[idx] = state->value;
		}
	}
};

struct HugeintSumOperation : public BaseSumOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->isset = true;
		state->value += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->isset = true;
		state->value += *input * hugeint_t(count);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->value;
		}
	}
};

unique_ptr<BaseStatistics> sum_propagate_stats(ClientContext &context, BoundAggregateExpression &expr, FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats, NodeStatistics *node_stats) {
	if (child_stats[0] && node_stats && node_stats->has_max_cardinality) {
		auto &numeric_stats = (NumericStatistics &) *child_stats[0];
		if (numeric_stats.max.is_null) {
			return nullptr;
		}
		auto max_sum_negative = hugeint_t(numeric_stats.min.GetValue<int64_t>()) * hugeint_t(node_stats->max_cardinality);
		auto max_sum_positive = hugeint_t(numeric_stats.max.GetValue<int64_t>()) * hugeint_t(node_stats->max_cardinality);
		if (max_sum_positive >= NumericLimits<int64_t>::Maximum() || max_sum_negative <= NumericLimits<int64_t>::Minimum()) {
			// sum can potentially exceed int64_t bounds: use hugeint sum
			return nullptr;
		}
		// total sum is guaranteed to fit in a single int64: use int64 sum instead of hugeint sum
		switch(expr.children[0]->return_type.InternalType()) {
		case PhysicalType::INT32:
			expr.function = AggregateFunction::UnaryAggregate<sum_state_t<int64_t>, int32_t, hugeint_t, IntegerSumOperation>(
		         LogicalType::INTEGER, LogicalType::HUGEINT);
			expr.function.name = "sum";
			break;
		case PhysicalType::INT64:
			expr.function = AggregateFunction::UnaryAggregate<sum_state_t<int64_t>, int64_t, hugeint_t, IntegerSumOperation>(
		         LogicalType::BIGINT, LogicalType::HUGEINT);
			expr.function.name = "sum";
			break;
		default:
			throw InternalException("Unsupported type for propagate sum stats");
		}
	}
	return nullptr;
}

AggregateFunction GetSumAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregate<sum_state_t<int64_t>, int16_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::SMALLINT, LogicalType::HUGEINT);
	case PhysicalType::INT32: {
		auto function = AggregateFunction::UnaryAggregate<hugeint_sum_state_t, int32_t, hugeint_t, SumToHugeintOperation>(
		    LogicalType::INTEGER, LogicalType::HUGEINT);
		function.statistics = sum_propagate_stats;
		return function;
	}
	case PhysicalType::INT64: {
		auto function = AggregateFunction::UnaryAggregate<hugeint_sum_state_t, int64_t, hugeint_t, SumToHugeintOperation>(
		    LogicalType::BIGINT, LogicalType::HUGEINT);
		function.statistics = sum_propagate_stats;
		return function;
	}
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregate<hugeint_sum_state_t, hugeint_t, hugeint_t, HugeintSumOperation>(
		    LogicalType::HUGEINT, LogicalType::HUGEINT);
	default:
		throw NotImplementedException("Unimplemented sum aggregate");
	}
}

unique_ptr<FunctionData> bind_decimal_sum(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetSumAggregate(decimal_type.InternalType());
	function.name = "sum";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType(LogicalTypeId::DECIMAL, Decimal::MAX_WIDTH_DECIMAL, decimal_type.scale());
	return nullptr;
}

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// decimal
	sum.AddFunction(AggregateFunction({LogicalType::DECIMAL}, LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, bind_decimal_sum));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT16));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT32));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT64));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT128));
	// float sums to float
	// FIXME: implement http://ic.ese.upenn.edu/pdf/parallel_fpaccum_tc2016.pdf for parallel FP sums
	sum.AddFunction(AggregateFunction::UnaryAggregate<numeric_sum_state_t, double, double, NumericSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	set.AddFunction(sum);
}

} // namespace duckdb
