#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/planner/expression.hpp"

using namespace std;

namespace duckdb {

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

struct sum_state_t {
	hugeint_t value;
	bool isset;
};

struct IntegerSumOperation : public BaseSumOperation {
	static void AddValue(sum_state_t *state, uint64_t value, int positive) {
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

struct numeric_sum_state_t {
	double value;
	bool isset;
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

struct hugeint_sum_state_t {
	hugeint_t value;
	bool isset;
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

AggregateFunction GetSumAggregate(LogicalType type) {
	// all integers sum to hugeint (FIXME: statistics for overflow prevention?)
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<sum_state_t, int16_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::SMALLINT, LogicalType::HUGEINT);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<sum_state_t, int32_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::INTEGER, LogicalType::HUGEINT);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<sum_state_t, int64_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::BIGINT, LogicalType::HUGEINT);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<hugeint_sum_state_t, hugeint_t, hugeint_t, HugeintSumOperation>(
		    LogicalType::HUGEINT, LogicalType::HUGEINT);
	default:
		throw NotImplementedException("Unimplemented sum aggregate");
	}
}

unique_ptr<FunctionData> bind_decimal_sum(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	switch(decimal_type.InternalType()) {
	case PhysicalType::INT16:
		function = GetSumAggregate(LogicalType::SMALLINT);
		break;
	case PhysicalType::INT32:
		function = GetSumAggregate(LogicalType::INTEGER);
		break;
	case PhysicalType::INT64:
		function = GetSumAggregate(LogicalType::BIGINT);
		break;
	default:
		function = GetSumAggregate(LogicalType::HUGEINT);
		break;
	}
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
	sum.AddFunction(GetSumAggregate(LogicalType::SMALLINT));
	sum.AddFunction(GetSumAggregate(LogicalType::INTEGER));
	sum.AddFunction(GetSumAggregate(LogicalType::BIGINT));
	sum.AddFunction(GetSumAggregate(LogicalType::HUGEINT));
	// float sums to float
	// FIXME: implement http://ic.ese.upenn.edu/pdf/parallel_fpaccum_tc2016.pdf for parallel FP sums
	sum.AddFunction(AggregateFunction::UnaryAggregate<numeric_sum_state_t, double, double, NumericSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	set.AddFunction(sum);
}

} // namespace duckdb
