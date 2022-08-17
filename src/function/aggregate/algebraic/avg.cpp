#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

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
		return make_unique<AverageDecimalBindData>(scale);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (AverageDecimalBindData &)other_p;
		return scale == other.scale;
	}
};

struct AverageSetOperation {
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
		state->count += count;
	}
};

template <class T>
static T GetAverageDivident(uint64_t count, FunctionData *bind_data) {
	T divident = T(count);
	if (bind_data) {
		auto &avg_bind_data = (AverageDecimalBindData &)*bind_data;
		divident *= avg_bind_data.scale;
	}
	return divident;
}

struct IntegerAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			double divident = GetAverageDivident<double>(state->count, aggr_input_data.bind_data);
			target[idx] = double(state->value) / divident;
		}
	}
};

struct IntegerAverageOperationHugeint : public BaseSumOperation<AverageSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			long double divident = GetAverageDivident<long double>(state->count, aggr_input_data.bind_data);
			target[idx] = Hugeint::Cast<long double>(state->value) / divident;
		}
	}
};

struct HugeintAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &aggr_input_data, STATE *state, T *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			long double divident = GetAverageDivident<long double>(state->count, aggr_input_data.bind_data);
			target[idx] = Hugeint::Cast<long double>(state->value) / divident;
		}
	}
};

struct NumericAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			if (!Value::DoubleIsFinite(state->value)) {
				throw OutOfRangeException("AVG is out of range!");
			}
			target[idx] = (state->value / state->count);
		}
	}
};

struct KahanAverageOperation : public BaseSumOperation<AverageSetOperation, KahanAdd> {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (state->count == 0) {
			mask.SetInvalid(idx);
		} else {
			if (!Value::DoubleIsFinite(state->value)) {
				throw OutOfRangeException("AVG is out of range!");
			}
			target[idx] = (state->value / state->count) + (state->err / state->count);
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
	function.return_type = LogicalType::DOUBLE;
	return make_unique<AverageDecimalBindData>(
	    Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[DecimalType::GetScale(decimal_type)]));
}

void AvgFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet avg("avg");

	avg.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalAvg));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT16));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT32));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT64));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT128));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<double>, double, double, NumericAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(avg);

	avg.name = "mean";
	set.AddFunction(avg);

	AggregateFunctionSet favg("favg");
	favg.AddFunction(AggregateFunction::UnaryAggregate<KahanAvgState, double, double, KahanAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(favg);
}

} // namespace duckdb
