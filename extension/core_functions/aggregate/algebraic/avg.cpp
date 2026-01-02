#include "core_functions/aggregate/algebraic_functions.hpp"
#include "core_functions/aggregate/sum_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

namespace {

template <class T>
struct AvgState {
	uint64_t count;
	T value;

	void Initialize() {
		this->count = 0;
	}

	void Combine(const AvgState<T> &other) {
		this->count += other.count;
		this->value += other.value;
	}
};

struct IntervalAvgState {
	int64_t count;
	interval_t value;

	void Initialize() {
		this->count = 0;
		this->value = interval_t();
	}

	void Combine(const IntervalAvgState &other) {
		this->count += other.count;
		this->value = AddOperator::Operation<interval_t, interval_t, interval_t>(this->value, other.value);
	}
};

struct KahanAvgState {
	uint64_t count;
	double value;
	double err;

	void Initialize() {
		this->count = 0;
		this->err = 0.0;
	}

	void Combine(const KahanAvgState &other) {
		this->count += other.count;
		KahanAddInternal(other.value, this->value, this->err);
		KahanAddInternal(other.err, this->value, this->err);
	}
};

struct AverageDecimalBindData : public FunctionData {
	explicit AverageDecimalBindData(double scale) : scale(scale) {
	}

	double scale;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<AverageDecimalBindData>(scale);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AverageDecimalBindData>();
		return scale == other.scale;
	}
};

struct AverageSetOperation {
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
		state.count += count;
	}
};

template <class T>
static T GetAverageDivident(uint64_t count, optional_ptr<FunctionData> bind_data) {
	T divident = T(count);
	if (bind_data) {
		auto &avg_bind_data = bind_data->Cast<AverageDecimalBindData>();
		divident *= avg_bind_data.scale;
	}
	return divident;
}

struct IntegerAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			double divident = GetAverageDivident<double>(state.count, finalize_data.input.bind_data);
			target = double(state.value) / divident;
		}
	}
};

struct IntegerAverageOperationHugeint : public BaseSumOperation<AverageSetOperation, AddToHugeint> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			long double divident = GetAverageDivident<long double>(state.count, finalize_data.input.bind_data);
			target = Hugeint::Cast<long double>(state.value) / divident;
		}
	}
};

struct DiscreteAverageOperation : public BaseSumOperation<AverageSetOperation, AddToHugeint> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			hugeint_t remainder;
			target = Hugeint::Cast<T>(Hugeint::DivMod(state.value, state.count, remainder));
			// Round the result
			target += (remainder > (state.count / 2));
		}
	}
};

struct HugeintAverageOperation : public BaseSumOperation<AverageSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			long double divident = GetAverageDivident<long double>(state.count, finalize_data.input.bind_data);
			target = Hugeint::Cast<long double>(state.value) / divident;
		}
	}
};

struct NumericAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.value / state.count;
		}
	}
};

struct KahanAverageOperation : public BaseSumOperation<AverageSetOperation, KahanAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = (state.value / state.count) + (state.err / state.count);
		}
	}
};

struct IntervalAverageOperation : public BaseSumOperation<AverageSetOperation, IntervalAdd> {
	// Override BaseSumOperation::Initialize because
	// IntervalAvgState does not have an assignment constructor from 0
	static void Initialize(IntervalAvgState &state) {
		AverageSetOperation::Initialize<IntervalAvgState>(state);
	}

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			// DivideOperator does not borrow fractions right,
			// TODO: Maybe it should?
			// Copy PG implementation.
			const auto &value = state.value;
			const auto count = UnsafeNumericCast<int64_t>(state.count);

			target.months = value.months / count;
			auto months_remainder = value.months % count;

			target.days = value.days / count;
			auto days_remainder = value.days % count;

			target.micros = value.micros / count;
			auto micros_remainder = value.micros % count;

			//	Shift the remainders right
			months_remainder *= Interval::DAYS_PER_MONTH;
			target.days += months_remainder / count;
			days_remainder += months_remainder % count;

			days_remainder *= Interval::MICROS_PER_DAY;
			micros_remainder += days_remainder / count;
			target.micros += micros_remainder;
		}
	}
};

struct TimeTZAverageOperation : public BaseSumOperation<AverageSetOperation, AddToHugeint> {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &aggr_unary) {
		const auto micros = Time::NormalizeTimeTZ(input).micros;
		AverageSetOperation::template AddValues<STATE>(state, 1);
		AddToHugeint::template AddNumber<STATE, int64_t>(state, micros);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &aggr_unary, idx_t count) {
		const auto micros = Time::NormalizeTimeTZ(input).micros;
		AverageSetOperation::template AddValues<STATE>(state, count);
		AddToHugeint::template AddConstant<STATE, int64_t>(state, micros, count);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			uint64_t remainder;
			auto micros = Hugeint::Cast<int64_t>(Hugeint::DivModPositive(state.value, state.count, remainder));
			// Round the result
			micros += (remainder > (state.count / 2));
			target = dtime_tz_t(dtime_t(micros), 0);
		}
	}
};

AggregateFunction GetAverageAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16: {
		return AggregateFunction::UnaryAggregate<AvgState<int64_t>, int16_t, double, IntegerAverageOperation>(
		    LogicalType::SMALLINT, LogicalType::DOUBLE);
	}
	case PhysicalType::INT32: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int32_t, double, IntegerAverageOperationHugeint>(
		    LogicalType::INTEGER, LogicalType::DOUBLE);
	}
	case PhysicalType::INT64: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, double, IntegerAverageOperationHugeint>(
		    LogicalType::BIGINT, LogicalType::DOUBLE);
	}
	case PhysicalType::INT128: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, hugeint_t, double, HugeintAverageOperation>(
		    LogicalType::HUGEINT, LogicalType::DOUBLE);
	}
	case PhysicalType::INTERVAL: {
		return AggregateFunction::UnaryAggregate<IntervalAvgState, interval_t, interval_t, IntervalAverageOperation>(
		    LogicalType::INTERVAL, LogicalType::INTERVAL);
	}
	default:
		throw InternalException("Unimplemented average aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalAvg(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetAverageAggregate(decimal_type.InternalType());
	function.name = "avg";
	function.arguments[0] = decimal_type;
	function.SetReturnType(LogicalType::DOUBLE);
	return make_uniq<AverageDecimalBindData>(
	    Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[DecimalType::GetScale(decimal_type)]));
}

} // namespace

AggregateFunctionSet AvgFun::GetFunctions() {
	AggregateFunctionSet avg;

	avg.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalAvg));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT16));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT32));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT64));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT128));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INTERVAL));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<double>, double, double, NumericAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));

	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, int64_t, DiscreteAverageOperation>(
	    LogicalType::TIMESTAMP, LogicalType::TIMESTAMP));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, int64_t, DiscreteAverageOperation>(
	    LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, int64_t, DiscreteAverageOperation>(
	    LogicalType::TIME, LogicalType::TIME));
	avg.AddFunction(
	    AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, dtime_tz_t, dtime_tz_t, TimeTZAverageOperation>(
	        LogicalType::TIME_TZ, LogicalType::TIME_TZ));

	return avg;
}

AggregateFunction FAvgFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KahanAvgState, double, double, KahanAverageOperation>(LogicalType::DOUBLE,
	                                                                                               LogicalType::DOUBLE);
}

} // namespace duckdb
