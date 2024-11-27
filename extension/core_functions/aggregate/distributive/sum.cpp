#include "core_functions/aggregate/distributive_functions.hpp"
#include "core_functions/aggregate/sum_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

struct SumSetOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.Initialize();
	}
	template <class STATE>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE &state, idx_t count) {
		state.isset = true;
	}
};

struct IntegerSumOperation : public BaseSumOperation<SumSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = Hugeint::Convert(state.value);
		}
	}
};

struct SumToHugeintOperation : public BaseSumOperation<SumSetOperation, AddToHugeint> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <class ADD_OPERATOR>
struct DoubleSumOperation : public BaseSumOperation<SumSetOperation, ADD_OPERATOR> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

using NumericSumOperation = DoubleSumOperation<RegularAdd>;
using KahanSumOperation = DoubleSumOperation<KahanAdd>;

struct HugeintSumOperation : public BaseSumOperation<SumSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

unique_ptr<FunctionData> SumNoOverflowBind(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	throw BinderException("sum_no_overflow is for internal use only!");
}

void SumNoOverflowSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                            const AggregateFunction &function) {
	return;
}

unique_ptr<FunctionData> SumNoOverflowDeserialize(Deserializer &deserializer, AggregateFunction &function) {
	function.return_type = deserializer.Get<const LogicalType &>();
	return nullptr;
}

AggregateFunction GetSumAggregateNoOverflow(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT32: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int32_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::INTEGER, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		function.bind = SumNoOverflowBind;
		function.serialize = SumNoOverflowSerialize;
		function.deserialize = SumNoOverflowDeserialize;
		return function;
	}
	case PhysicalType::INT64: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int64_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::BIGINT, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		function.bind = SumNoOverflowBind;
		function.serialize = SumNoOverflowSerialize;
		function.deserialize = SumNoOverflowDeserialize;
		return function;
	}
	default:
		throw BinderException("Unsupported internal type for sum_no_overflow");
	}
}

AggregateFunction GetSumAggregateNoOverflowDecimal() {
	AggregateFunction aggr({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr,
	                       nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, SumNoOverflowBind);
	aggr.serialize = SumNoOverflowSerialize;
	aggr.deserialize = SumNoOverflowDeserialize;
	return aggr;
}

unique_ptr<BaseStatistics> SumPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                             AggregateStatisticsInput &input) {
	if (input.node_stats && input.node_stats->has_max_cardinality) {
		auto &numeric_stats = input.child_stats[0];
		if (!NumericStats::HasMinMax(numeric_stats)) {
			return nullptr;
		}
		auto internal_type = numeric_stats.GetType().InternalType();
		hugeint_t max_negative;
		hugeint_t max_positive;
		switch (internal_type) {
		case PhysicalType::INT32:
			max_negative = NumericStats::Min(numeric_stats).GetValueUnsafe<int32_t>();
			max_positive = NumericStats::Max(numeric_stats).GetValueUnsafe<int32_t>();
			break;
		case PhysicalType::INT64:
			max_negative = NumericStats::Min(numeric_stats).GetValueUnsafe<int64_t>();
			max_positive = NumericStats::Max(numeric_stats).GetValueUnsafe<int64_t>();
			break;
		default:
			throw InternalException("Unsupported type for propagate sum stats");
		}
		auto max_sum_negative = max_negative * Hugeint::Convert(input.node_stats->max_cardinality);
		auto max_sum_positive = max_positive * Hugeint::Convert(input.node_stats->max_cardinality);
		if (max_sum_positive >= NumericLimits<int64_t>::Maximum() ||
		    max_sum_negative <= NumericLimits<int64_t>::Minimum()) {
			// sum can potentially exceed int64_t bounds: use hugeint sum
			return nullptr;
		}
		// total sum is guaranteed to fit in a single int64: use int64 sum instead of hugeint sum
		expr.function = GetSumAggregateNoOverflow(internal_type);
	}
	return nullptr;
}

AggregateFunction GetSumAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int16_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::SMALLINT, LogicalType::HUGEINT);
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}

	case PhysicalType::INT32: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int32_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::INTEGER, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	case PhysicalType::INT64: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int64_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::BIGINT, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	case PhysicalType::INT128: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, hugeint_t, hugeint_t, HugeintSumOperation>(
		        LogicalType::HUGEINT, LogicalType::HUGEINT);
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	default:
		throw InternalException("Unimplemented sum aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalSum(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetSumAggregate(decimal_type.InternalType());
	function.name = "sum";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return nullptr;
}

AggregateFunctionSet SumFun::GetFunctions() {
	AggregateFunctionSet sum;
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
	return sum;
}

AggregateFunctionSet SumNoOverflowFun::GetFunctions() {
	AggregateFunctionSet sum_no_overflow;
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT32));
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT64));
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflowDecimal());
	return sum_no_overflow;
}

AggregateFunction KahanSumFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KahanSumState, double, double, KahanSumOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE);
}

} // namespace duckdb
