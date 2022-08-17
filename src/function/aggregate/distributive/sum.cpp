#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/algebraic_functions.hpp"

namespace duckdb {

struct SumSetOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->Initialize();
	}
	template <class STATE>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE *state, idx_t count) {
		state->isset = true;
	}
};

struct IntegerSumOperation : public BaseSumOperation<SumSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = Hugeint::Convert(state->value);
		}
	}
};

struct SumToHugeintOperation : public BaseSumOperation<SumSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}
};

template <class ADD_OPERATOR>
struct DoubleSumOperation : public BaseSumOperation<SumSetOperation, ADD_OPERATOR> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetInvalid(idx);
		} else {
			if (!Value::DoubleIsFinite(state->value)) {
				throw OutOfRangeException("SUM is out of range!");
			}
			target[idx] = state->value;
		}
	}
};

using NumericSumOperation = DoubleSumOperation<RegularAdd>;
using KahanSumOperation = DoubleSumOperation<KahanAdd>;

struct HugeintSumOperation : public BaseSumOperation<SumSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}
};

unique_ptr<BaseStatistics> SumPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                             FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats,
                                             NodeStatistics *node_stats) {
	if (child_stats[0] && node_stats && node_stats->has_max_cardinality) {
		auto &numeric_stats = (NumericStatistics &)*child_stats[0];
		if (numeric_stats.min.IsNull() || numeric_stats.max.IsNull()) {
			return nullptr;
		}
		auto internal_type = numeric_stats.min.type().InternalType();
		hugeint_t max_negative;
		hugeint_t max_positive;
		switch (internal_type) {
		case PhysicalType::INT32:
			max_negative = numeric_stats.min.GetValueUnsafe<int32_t>();
			max_positive = numeric_stats.max.GetValueUnsafe<int32_t>();
			break;
		case PhysicalType::INT64:
			max_negative = numeric_stats.min.GetValueUnsafe<int64_t>();
			max_positive = numeric_stats.max.GetValueUnsafe<int64_t>();
			break;
		default:
			throw InternalException("Unsupported type for propagate sum stats");
		}
		auto max_sum_negative = max_negative * hugeint_t(node_stats->max_cardinality);
		auto max_sum_positive = max_positive * hugeint_t(node_stats->max_cardinality);
		if (max_sum_positive >= NumericLimits<int64_t>::Maximum() ||
		    max_sum_negative <= NumericLimits<int64_t>::Minimum()) {
			// sum can potentially exceed int64_t bounds: use hugeint sum
			return nullptr;
		}
		// total sum is guaranteed to fit in a single int64: use int64 sum instead of hugeint sum
		expr.function = SumFun::GetSumAggregateNoOverflow(internal_type);
	}
	return nullptr;
}

AggregateFunction SumFun::GetSumAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int16_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::SMALLINT, LogicalType::HUGEINT);
		return function;
	}

	case PhysicalType::INT32: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int32_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::INTEGER, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		return function;
	}
	case PhysicalType::INT64: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int64_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::BIGINT, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		return function;
	}
	case PhysicalType::INT128: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, hugeint_t, hugeint_t, HugeintSumOperation>(
		        LogicalType::HUGEINT, LogicalType::HUGEINT);
		return function;
	}
	default:
		throw InternalException("Unimplemented sum aggregate");
	}
}

AggregateFunction SumFun::GetSumAggregateNoOverflow(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT32: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int32_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::INTEGER, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		return function;
	}
	case PhysicalType::INT64: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int64_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::BIGINT, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		return function;
	}
	default:
		throw BinderException("Unsupported internal type for sum_no_overflow");
	}
}

unique_ptr<FunctionData> BindDecimalSum(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = SumFun::GetSumAggregate(decimal_type.InternalType());
	function.name = "sum";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	return nullptr;
}

unique_ptr<FunctionData> BindDecimalSumNoOverflow(ClientContext &context, AggregateFunction &function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = SumFun::GetSumAggregateNoOverflow(decimal_type.InternalType());
	function.name = "sum_no_overflow";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	return nullptr;
}

void SumFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sum("sum");
	// decimal
	sum.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalSum));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT16));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT32));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT64));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT128));
	sum.AddFunction(AggregateFunction::UnaryAggregate<SumState<double>, double, double, NumericSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	set.AddFunction(sum);

	AggregateFunctionSet sum_no_overflow("sum_no_overflow");
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT32));
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT64));
	sum_no_overflow.AddFunction(
	    AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalSumNoOverflow));
	set.AddFunction(sum_no_overflow);

	// fsum
	AggregateFunctionSet fsum("fsum");
	fsum.AddFunction(AggregateFunction::UnaryAggregate<KahanSumState, double, double, KahanSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	set.AddFunction(fsum);

	fsum.name = "kahan_sum";
	set.AddFunction(fsum);

	fsum.name = "sumKahan";
	set.AddFunction(fsum);
}

} // namespace duckdb
