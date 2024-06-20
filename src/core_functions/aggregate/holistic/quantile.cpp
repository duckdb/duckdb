#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/quantile_enum.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/abs.hpp"
#include "duckdb/core_functions/aggregate/quantile_state.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "SkipList.h"

namespace duckdb {

template <class INPUT_TYPE>
struct IndirectLess {
	inline explicit IndirectLess(const INPUT_TYPE *inputs_p) : inputs(inputs_p) {
	}

	inline bool operator()(const idx_t &lhi, const idx_t &rhi) const {
		return inputs[lhi] < inputs[rhi];
	}

	const INPUT_TYPE *inputs;
};

template <typename T>
static inline T QuantileAbs(const T &t) {
	return AbsOperator::Operation<T, T>(t);
}

template <>
inline Value QuantileAbs(const Value &v) {
	const auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::DECIMAL: {
		const auto integral = IntegralValue::Get(v);
		const auto width = DecimalType::GetWidth(type);
		const auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(QuantileAbs<int16_t>(Cast::Operation<hugeint_t, int16_t>(integral)), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(QuantileAbs<int32_t>(Cast::Operation<hugeint_t, int32_t>(integral)), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(QuantileAbs<int64_t>(Cast::Operation<hugeint_t, int64_t>(integral)), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(QuantileAbs<hugeint_t>(integral), width, scale);
		default:
			throw InternalException("Unknown DECIMAL type");
		}
	}
	default:
		return Value::DOUBLE(QuantileAbs<double>(v.GetValue<double>()));
	}
}

void BindQuantileInner(AggregateFunction &function, const LogicalType &type, QuantileSerializationType quantile_type);


//===--------------------------------------------------------------------===//
// Quantile Bind Data
//===--------------------------------------------------------------------===//
QuantileBindData::QuantileBindData() {
}

QuantileBindData::QuantileBindData(const Value &quantile_p)
    : quantiles(1, QuantileValue(QuantileAbs(quantile_p))), order(1, 0), desc(quantile_p < 0) {
}

QuantileBindData::QuantileBindData(const vector<Value> &quantiles_p) {
	vector<Value> normalised;
	size_t pos = 0;
	size_t neg = 0;
	for (idx_t i = 0; i < quantiles_p.size(); ++i) {
		const auto &q = quantiles_p[i];
		pos += (q > 0);
		neg += (q < 0);
		normalised.emplace_back(QuantileAbs(q));
		order.push_back(i);
	}
	if (pos && neg) {
		throw BinderException("QUANTILE parameters must have consistent signs");
	}
	desc = (neg > 0);

	IndirectLess<Value> lt(normalised.data());
	std::sort(order.begin(), order.end(), lt);

	for (const auto &q : normalised) {
		quantiles.emplace_back(QuantileValue(q));
	}
}

QuantileBindData::QuantileBindData(const QuantileBindData &other) : order(other.order), desc(other.desc) {
	for (const auto &q : other.quantiles) {
		quantiles.emplace_back(q);
	}
}

unique_ptr<FunctionData> QuantileBindData::Copy() const {
	return make_uniq<QuantileBindData>(*this);
}

bool QuantileBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<QuantileBindData>();
	return desc == other.desc && quantiles == other.quantiles && order == other.order;
}

void QuantileBindData::Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                      const AggregateFunction &function) {
	auto &bind_data = bind_data_p->Cast<QuantileBindData>();
	vector<Value> raw;
	for (const auto &q : bind_data.quantiles) {
		raw.emplace_back(q.val);
	}
	serializer.WriteProperty(100, "quantiles", raw);
	serializer.WriteProperty(101, "order", bind_data.order);
	serializer.WriteProperty(102, "desc", bind_data.desc);
}

unique_ptr<FunctionData> QuantileBindData::Deserialize(Deserializer &deserializer, AggregateFunction &function) {
	auto result = make_uniq<QuantileBindData>();
	vector<Value> raw;
	deserializer.ReadProperty(100, "quantiles", raw);
	deserializer.ReadProperty(101, "order", result->order);
	deserializer.ReadProperty(102, "desc", result->desc);
	QuantileSerializationType deserialization_type;
	deserializer.ReadPropertyWithDefault(103, "quantile_type", deserialization_type,
	                                     QuantileSerializationType::NON_DECIMAL);

	if (deserialization_type != QuantileSerializationType::NON_DECIMAL) {
		LogicalType arg_type;
		deserializer.ReadProperty(104, "logical_type", arg_type);

		BindQuantileInner(function, arg_type, deserialization_type);
	}

	for (const auto &r : raw) {
		result->quantiles.emplace_back(QuantileValue(r));
	}
	return std::move(result);
}

void QuantileBindData::SerializeDecimalDiscrete(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                     const AggregateFunction &function) {
	Serialize(serializer, bind_data_p, function);

	serializer.WritePropertyWithDefault<QuantileSerializationType>(
	    103, "quantile_type", QuantileSerializationType::DECIMAL_DISCRETE, QuantileSerializationType::NON_DECIMAL);
	serializer.WriteProperty(104, "logical_type", function.arguments[0]);
}
void QuantileBindData::SerializeDecimalDiscreteList(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                         const AggregateFunction &function) {

	Serialize(serializer, bind_data_p, function);

	serializer.WritePropertyWithDefault<QuantileSerializationType>(103, "quantile_type",
	                                                               QuantileSerializationType::DECIMAL_DISCRETE_LIST,
	                                                               QuantileSerializationType::NON_DECIMAL);
	serializer.WriteProperty(104, "logical_type", function.arguments[0]);
}
void QuantileBindData::SerializeDecimalContinuous(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                       const AggregateFunction &function) {
	Serialize(serializer, bind_data_p, function);

	serializer.WritePropertyWithDefault<QuantileSerializationType>(103, "quantile_type",
	                                                               QuantileSerializationType::DECIMAL_CONTINUOUS,
	                                                               QuantileSerializationType::NON_DECIMAL);
	serializer.WriteProperty(104, "logical_type", function.arguments[0]);
}
void QuantileBindData::SerializeDecimalContinuousList(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                           const AggregateFunction &function) {

	Serialize(serializer, bind_data_p, function);

	serializer.WritePropertyWithDefault<QuantileSerializationType>(
	    103, "quantile_type", QuantileSerializationType::DECIMAL_CONTINUOUS_LIST,
	    QuantileSerializationType::NON_DECIMAL);
	serializer.WriteProperty(104, "logical_type", function.arguments[0]);
}

//===--------------------------------------------------------------------===//
// Cast Interpolation
//===--------------------------------------------------------------------===//
template <>
interval_t CastInterpolation::Cast(const dtime_t &src, Vector &result) {
	return {0, 0, src.micros};
}

template <>
double CastInterpolation::Interpolate(const double &lo, const double d, const double &hi) {
	return lo * (1.0 - d) + hi * d;
}

template <>
dtime_t CastInterpolation::Interpolate(const dtime_t &lo, const double d, const dtime_t &hi) {
	return dtime_t(std::llround(lo.micros * (1.0 - d) + hi.micros * d));
}

template <>
timestamp_t CastInterpolation::Interpolate(const timestamp_t &lo, const double d, const timestamp_t &hi) {
	return timestamp_t(std::llround(lo.value * (1.0 - d) + hi.value * d));
}

template <>
hugeint_t CastInterpolation::Interpolate(const hugeint_t &lo, const double d, const hugeint_t &hi) {
	return Hugeint::Convert(Interpolate(Hugeint::Cast<double>(lo), d, Hugeint::Cast<double>(hi)));
}

static interval_t MultiplyByDouble(const interval_t &i, const double &d) { // NOLINT
	D_ASSERT(d >= 0 && d <= 1);
	return Interval::FromMicro(std::llround(Interval::GetMicro(i) * d));
}

inline interval_t operator+(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) + Interval::GetMicro(rhs));
}

inline interval_t operator-(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) - Interval::GetMicro(rhs));
}

template <>
interval_t CastInterpolation::Interpolate(const interval_t &lo, const double d, const interval_t &hi) {
	const interval_t delta = hi - lo;
	return lo + MultiplyByDouble(delta, d);
}

template <>
string_t CastInterpolation::Cast(const std::string &src, Vector &result) {
	return StringVector::AddString(result, src);
}

template <>
string_t CastInterpolation::Cast(const string_t &src, Vector &result) {
	return StringVector::AddString(result, src);
}

//===--------------------------------------------------------------------===//
// Quantile State
//===--------------------------------------------------------------------===//
template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) { // NOLINT
	LogicalType result_type =
	    LogicalType::LIST(child_type.id() == LogicalTypeId::ANY ? LogicalType::VARCHAR : child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <bool DISCRETE>
struct QuantileScalarOperation : QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		Interpolator<DISCRETE> interp(bind_data.quantiles[0], state.v.size(), bind_data.desc);
		target = interp.template Operation<typename STATE::SaveType, T>(state.v.data(), finalize_data.result);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &result,
	                   idx_t ridx, const STATE *gstate) {
		QuantileIncluded included(fmask, dmask);
		const auto n = FrameSize(included, frames);

		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		if (!n) {
			rmask.Set(ridx, false);
			return;
		}

		const auto &quantile = bind_data.quantiles[0];
		if (gstate && gstate->HasTrees()) {
			rdata[ridx] = gstate->GetWindowState().template WindowScalar<RESULT_TYPE, DISCRETE>(data, frames, n, result, quantile);
		} else {
			auto &window_state = state.GetOrCreateWindowState();

			//	Update the skip list
			window_state.UpdateSkip(data, frames, included);

			// Find the position(s) needed
			rdata[ridx] = window_state.template WindowScalar<RESULT_TYPE, DISCRETE>(data, frames, n, result, quantile);

			//	Save the previous state for next time
			window_state.prevs = frames;
		}
	}
};

template <typename INPUT_TYPE, typename SAVED_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<INPUT_TYPE, SAVED_TYPE>;
	using OP = QuantileScalarOperation<true>;
	auto return_type = type.id() == LogicalTypeId::ANY ? LogicalType::VARCHAR : type;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, return_type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	fun.window_init = OP::WindowInit<STATE, INPUT_TYPE>;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileAggregateFunction<interval_t, interval_t>(type);
	case LogicalTypeId::ANY:
		return GetTypedDiscreteQuantileAggregateFunction<string_t, std::string>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile aggregate");
	}
}

template <class CHILD_TYPE, bool DISCRETE>
struct QuantileListOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state.v.data();
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		idx_t lower = 0;
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<DISCRETE> interp(quantile, state.v.size(), bind_data.desc);
			interp.begin = lower;
			rdata[ridx + q] = interp.template Operation<typename STATE::SaveType, CHILD_TYPE>(v_t, result);
			lower = interp.FRN;
		}
		entry.length = bind_data.quantiles.size();

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const SubFrames &frames, Vector &list,
	                   idx_t lidx, const STATE *gstate) {
		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		QuantileIncluded included(fmask, dmask);
		const auto n = FrameSize(included, frames);

		// Result is a constant LIST<RESULT_TYPE> with a fixed length
		if (!n) {
			auto &lmask = FlatVector::Validity(list);
			lmask.Set(lidx, false);
			return;
		}

		if (gstate && gstate->HasTrees()) {
			gstate->GetWindowState().template WindowList<CHILD_TYPE, DISCRETE>(data, frames, n, list, lidx, bind_data);
		} else {
			auto &window_state = state.GetOrCreateWindowState();
			window_state.UpdateSkip(data, frames, included);
			window_state.template WindowList<CHILD_TYPE, DISCRETE>(data, frames, n, list, lidx, bind_data);
			window_state.prevs = frames;
		}
	}
};

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<INPUT_TYPE, SAVE_TYPE>;
	using OP = QuantileListOperation<INPUT_TYPE, true>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t, date_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t, timestamp_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedDiscreteQuantileListAggregateFunction<dtime_t, dtime_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileListAggregateFunction<interval_t, interval_t>(type);
	case LogicalTypeId::ANY:
		return GetTypedDiscreteQuantileListAggregateFunction<string_t, std::string>(type);
	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileAggregateFunction(const LogicalType &input_type,
                                                              const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE, INPUT_TYPE>;
	using OP = QuantileScalarOperation<false>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented continuous quantile DECIMAL aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented continuous quantile aggregate");
	}
}

template <typename INPUT_TYPE, typename CHILD_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	using STATE = QuantileState<INPUT_TYPE, INPUT_TYPE>;
	using OP = QuantileListOperation<CHILD_TYPE, false>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, result_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileListAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileListAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileListAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileListAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileListAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileListAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileListAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileListAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileListAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile DECIMAL list aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileListAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileListAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedContinuousQuantileListAggregateFunction<dtime_t, dtime_t>(type, type);
	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

static const Value &CheckQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < -1 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [-1, 1]");
	}
	if (Value::IsNan(quantile)) {
		throw BinderException("QUANTILE parameter cannot be NaN");
	}

	return quantile_val;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (quantile_val.IsNull()) {
		throw BinderException("QUANTILE argument must not be NULL");
	}
	vector<Value> quantiles;
	switch (quantile_val.type().id()) {
	case LogicalTypeId::LIST:
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckQuantile(element_val));
		}
		break;
	case LogicalTypeId::ARRAY:
		for (const auto &element_val : ArrayValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckQuantile(element_val));
		}
		break;
	default:
		quantiles.push_back(CheckQuantile(quantile_val));
		break;
	}

	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<QuantileBindData>(quantiles);
}

void BindQuantileInner(AggregateFunction &function, const LogicalType &type, QuantileSerializationType quantile_type) {
	switch (quantile_type) {
	case QuantileSerializationType::DECIMAL_DISCRETE:
		function = GetDiscreteQuantileAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalDiscrete;
		function.name = "quantile_disc";
		break;
	case QuantileSerializationType::DECIMAL_DISCRETE_LIST:
		function = GetDiscreteQuantileListAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalDiscreteList;
		function.name = "quantile_disc";
		break;
	case QuantileSerializationType::DECIMAL_CONTINUOUS:
		function = GetContinuousQuantileAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalContinuous;
		function.name = "quantile_cont";
		break;
	case QuantileSerializationType::DECIMAL_CONTINUOUS_LIST:
		function = GetContinuousQuantileListAggregateFunction(type);
		function.serialize = QuantileBindData::SerializeDecimalContinuousList;
		function.name = "quantile_cont";
		break;
	case QuantileSerializationType::NON_DECIMAL:
		throw SerializationException("NON_DECIMAL is not a valid quantile_type for BindQuantileInner");
	}
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_DISCRETE);
	return bind_data;
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_DISCRETE_LIST);
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_CONTINUOUS);
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	BindQuantileInner(function, arguments[0]->return_type, QuantileSerializationType::DECIMAL_CONTINUOUS_LIST);
	return bind_data;
}
static bool CanInterpolate(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

unique_ptr<FunctionData> DeserializeMedian(Deserializer &deserializer, AggregateFunction &function);

AggregateFunction GetMedianAggregate(const LogicalType &type) {
	auto fun = CanInterpolate(type) ? GetContinuousQuantileAggregateFunction(type)
	                                : GetDiscreteQuantileAggregateFunction(type);
	fun.name = "median";
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = DeserializeMedian;
	return fun;
}

unique_ptr<FunctionData> DeserializeMedian(Deserializer &deserializer, AggregateFunction &function) {
	auto bind_data = QuantileBindData::Deserialize(deserializer, function);

	auto &input_type = function.arguments[0];
	function = GetMedianAggregate(input_type);
	return bind_data;
}

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	function = GetMedianAggregate(arguments[0]->return_type);
	return make_uniq<QuantileBindData>(Value::DECIMAL(int16_t(5), 2, 1));
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetQuantileDecimalAggregate(const vector<LogicalType> &arguments, const LogicalType &return_type,
                                              bind_aggregate_function_t bind) {
	AggregateFunction fun(arguments, return_type, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, bind);
	fun.bind = bind;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

vector<LogicalType> GetQuantileTypes() {
	return {LogicalType::TINYINT,      LogicalType::SMALLINT,
	        LogicalType::INTEGER,      LogicalType::BIGINT,
	        LogicalType::HUGEINT,      LogicalType::FLOAT,
	        LogicalType::DOUBLE,       LogicalType::DATE,
	        LogicalType::TIMESTAMP,    LogicalType::TIME,
	        LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,
	        LogicalType::INTERVAL,     LogicalType::ANY_PARAMS(LogicalType::VARCHAR, 150)};
}

AggregateFunctionSet MedianFun::GetFunctions() {
	AggregateFunctionSet median("median");
	AggregateFunction fun({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, BindMedian);
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = DeserializeMedian;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	median.AddFunction(fun);
	return median;
}

AggregateFunctionSet QuantileDiscFun::GetFunctions() {
	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindDiscreteQuantileDecimal));
	quantile_disc.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindDiscreteQuantileDecimalList));
	for (const auto &type : GetQuantileTypes()) {
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
	}
	return quantile_disc;
	// quantile
}

AggregateFunctionSet QuantileContFun::GetFunctions() {
	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindContinuousQuantileDecimal));
	quantile_cont.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindContinuousQuantileDecimalList));

	for (const auto &type : GetQuantileTypes()) {
		if (CanInterpolate(type)) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
		}
	}
	return quantile_cont;
}

} // namespace duckdb
