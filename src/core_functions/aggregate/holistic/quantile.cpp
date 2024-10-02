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
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"

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
	deserializer.ReadPropertyWithExplicitDefault(103, "quantile_type", deserialization_type,
	                                             QuantileSerializationType::NON_DECIMAL);

	if (deserialization_type != QuantileSerializationType::NON_DECIMAL) {
		deserializer.ReadDeletedProperty<LogicalType>(104, "logical_type");
	}

	for (const auto &r : raw) {
		result->quantiles.emplace_back(QuantileValue(r));
	}
	return std::move(result);
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
	return dtime_t(std::llround(static_cast<double>(lo.micros) * (1.0 - d) + static_cast<double>(hi.micros) * d));
}

template <>
timestamp_t CastInterpolation::Interpolate(const timestamp_t &lo, const double d, const timestamp_t &hi) {
	return timestamp_t(std::llround(static_cast<double>(lo.value) * (1.0 - d) + static_cast<double>(hi.value) * d));
}

template <>
hugeint_t CastInterpolation::Interpolate(const hugeint_t &lo, const double d, const hugeint_t &hi) {
	return Hugeint::Convert(Interpolate(Hugeint::Cast<double>(lo), d, Hugeint::Cast<double>(hi)));
}

static interval_t MultiplyByDouble(const interval_t &i, const double &d) { // NOLINT
	D_ASSERT(d >= 0 && d <= 1);
	return Interval::FromMicro(std::llround(static_cast<double>(Interval::GetMicro(i)) * d));
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
string_t CastInterpolation::Cast(const string_t &src, Vector &result) {
	return StringVector::AddStringOrBlob(result, src);
}

//===--------------------------------------------------------------------===//
// Scalar Quantile
//===--------------------------------------------------------------------===//
template <bool DISCRETE, class TYPE_OP = QuantileStandardType>
struct QuantileScalarOperation : public QuantileOperation {
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
		target = interp.template Operation<typename STATE::InputType, T>(state.v.data(), finalize_data.result);
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
			rdata[ridx] = gstate->GetWindowState().template WindowScalar<RESULT_TYPE, DISCRETE>(data, frames, n, result,
			                                                                                    quantile);
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

struct QuantileScalarFallback : QuantileOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Execute(STATE &state, const INPUT_TYPE &key, AggregateInputData &input_data) {
		state.AddElement(key, input_data);
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		Interpolator<true> interp(bind_data.quantiles[0], state.v.size(), bind_data.desc);
		auto interpolation_result = interp.InterpolateInternal<string_t>(state.v.data());
		CreateSortKeyHelpers::DecodeSortKey(interpolation_result, finalize_data.result, finalize_data.result_idx,
		                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	}
};

//===--------------------------------------------------------------------===//
// Quantile List
//===--------------------------------------------------------------------===//
template <class CHILD_TYPE, bool DISCRETE>
struct QuantileListOperation : QuantileOperation {
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
			rdata[ridx + q] = interp.template Operation<typename STATE::InputType, CHILD_TYPE>(v_t, result);
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

struct QuantileListFallback : QuantileOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Execute(STATE &state, const INPUT_TYPE &key, AggregateInputData &input_data) {
		state.AddElement(key, input_data);
	}

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

		D_ASSERT(state.v.data());

		auto &entry = target;
		entry.offset = ridx;
		idx_t lower = 0;
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<true> interp(quantile, state.v.size(), bind_data.desc);
			interp.begin = lower;
			auto interpolation_result = interp.InterpolateInternal<string_t>(state.v.data());
			CreateSortKeyHelpers::DecodeSortKey(interpolation_result, result, ridx + q,
			                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
			lower = interp.FRN;
		}
		entry.length = bind_data.quantiles.size();

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

//===--------------------------------------------------------------------===//
// Discrete Quantiles
//===--------------------------------------------------------------------===//
template <class OP>
AggregateFunction GetDiscreteQuantileTemplated(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return OP::template GetFunction<int8_t>(type);
	case PhysicalType::INT16:
		return OP::template GetFunction<int16_t>(type);
	case PhysicalType::INT32:
		return OP::template GetFunction<int32_t>(type);
	case PhysicalType::INT64:
		return OP::template GetFunction<int64_t>(type);
	case PhysicalType::INT128:
		return OP::template GetFunction<hugeint_t>(type);
	case PhysicalType::FLOAT:
		return OP::template GetFunction<float>(type);
	case PhysicalType::DOUBLE:
		return OP::template GetFunction<double>(type);
	case PhysicalType::INTERVAL:
		return OP::template GetFunction<interval_t>(type);
	case PhysicalType::VARCHAR:
		return OP::template GetFunction<string_t, QuantileStringType>(type);
	default:
		return OP::GetFallback(type);
	}
}

struct ScalarDiscreteQuantile {
	template <typename INPUT_TYPE, class TYPE_OP = QuantileStandardType>
	static AggregateFunction GetFunction(const LogicalType &type) {
		using STATE = QuantileState<INPUT_TYPE, TYPE_OP>;
		using OP = QuantileScalarOperation<true>;
		auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
		fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
		fun.window_init = OP::WindowInit<STATE, INPUT_TYPE>;
		return fun;
	}

	static AggregateFunction GetFallback(const LogicalType &type) {
		using STATE = QuantileState<string_t, QuantileStringType>;
		using OP = QuantileScalarFallback;

		AggregateFunction fun(
		    {type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    AggregateSortKeyHelpers::UnaryUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
		    AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, nullptr,
		    AggregateFunction::StateDestroy<STATE, OP>);
		return fun;
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) { // NOLINT
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

struct ListDiscreteQuantile {
	template <typename INPUT_TYPE, class TYPE_OP = QuantileStandardType>
	static AggregateFunction GetFunction(const LogicalType &type) {
		using STATE = QuantileState<INPUT_TYPE, TYPE_OP>;
		using OP = QuantileListOperation<INPUT_TYPE, true>;
		auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
		fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
		return fun;
	}

	static AggregateFunction GetFallback(const LogicalType &type) {
		using STATE = QuantileState<string_t, QuantileStringType>;
		using OP = QuantileListFallback;

		AggregateFunction fun(
		    {type}, LogicalType::LIST(type), AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateSortKeyHelpers::UnaryUpdate<STATE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, list_entry_t, OP>,
		    nullptr, nullptr, AggregateFunction::StateDestroy<STATE, OP>);
		return fun;
	}
};

AggregateFunction GetDiscreteQuantile(const LogicalType &type) {
	return GetDiscreteQuantileTemplated<ScalarDiscreteQuantile>(type);
}

AggregateFunction GetDiscreteQuantileList(const LogicalType &type) {
	return GetDiscreteQuantileTemplated<ListDiscreteQuantile>(type);
}

//===--------------------------------------------------------------------===//
// Continuous Quantiles
//===--------------------------------------------------------------------===//
template <class OP>
AggregateFunction GetContinuousQuantileTemplated(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return OP::template GetFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return OP::template GetFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::INTEGER:
		return OP::template GetFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return OP::template GetFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return OP::template GetFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::FLOAT:
		return OP::template GetFunction<float, float>(type, type);
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DOUBLE:
		return OP::template GetFunction<double, double>(LogicalType::DOUBLE, LogicalType::DOUBLE);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return OP::template GetFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return OP::template GetFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return OP::template GetFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return OP::template GetFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented continuous quantile DECIMAL aggregate");
		}
	case LogicalTypeId::DATE:
		return OP::template GetFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return OP::template GetFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return OP::template GetFunction<dtime_t, dtime_t>(type, type);
	default:
		throw NotImplementedException("Unimplemented continuous quantile aggregate");
	}
}

struct ScalarContinuousQuantile {
	template <typename INPUT_TYPE, typename TARGET_TYPE>
	static AggregateFunction GetFunction(const LogicalType &input_type, const LogicalType &target_type) {
		using STATE = QuantileState<INPUT_TYPE, QuantileStandardType>;
		using OP = QuantileScalarOperation<false>;
		auto fun =
		    AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
		fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
		return fun;
	}
};

struct ListContinuousQuantile {
	template <typename INPUT_TYPE, typename TARGET_TYPE>
	static AggregateFunction GetFunction(const LogicalType &input_type, const LogicalType &target_type) {
		using STATE = QuantileState<INPUT_TYPE, QuantileStandardType>;
		using OP = QuantileListOperation<TARGET_TYPE, false>;
		auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, target_type);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
		fun.window_init = OP::template WindowInit<STATE, INPUT_TYPE>;
		return fun;
	}
};

AggregateFunction GetContinuousQuantile(const LogicalType &type) {
	return GetContinuousQuantileTemplated<ScalarContinuousQuantile>(type);
}

AggregateFunction GetContinuousQuantileList(const LogicalType &type) {
	return GetContinuousQuantileTemplated<ListContinuousQuantile>(type);
}

//===--------------------------------------------------------------------===//
// Quantile binding
//===--------------------------------------------------------------------===//
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
	if (arguments.size() < 2) {
		throw BinderException("QUANTILE requires a range argument between [0, 1]");
	}
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

//===--------------------------------------------------------------------===//
// Function definitions
//===--------------------------------------------------------------------===//
static bool CanInterpolate(const LogicalType &type) {
	if (type.HasAlias()) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return true;
	default:
		return false;
	}
}

struct MedianFunction {
	static AggregateFunction GetAggregate(const LogicalType &type) {
		auto fun = CanInterpolate(type) ? GetContinuousQuantile(type) : GetDiscreteQuantile(type);
		fun.name = "median";
		fun.serialize = QuantileBindData::Serialize;
		fun.deserialize = Deserialize;
		return fun;
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto bind_data = QuantileBindData::Deserialize(deserializer, function);

		auto &input_type = function.arguments[0];
		function = GetAggregate(input_type);
		return bind_data;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function = GetAggregate(arguments[0]->return_type);
		return make_uniq<QuantileBindData>(Value::DECIMAL(int16_t(5), 2, 1));
	}
};

struct DiscreteQuantileListFunction {
	static AggregateFunction GetAggregate(const LogicalType &type) {
		auto fun = GetDiscreteQuantileList(type);
		fun.name = "quantile_disc";
		fun.bind = Bind;
		fun.serialize = QuantileBindData::Serialize;
		fun.deserialize = Deserialize;
		// temporarily push an argument so we can bind the actual quantile
		fun.arguments.emplace_back(LogicalType::LIST(LogicalType::DOUBLE));
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return fun;
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto bind_data = QuantileBindData::Deserialize(deserializer, function);

		auto &input_type = function.arguments[0];
		function = GetAggregate(input_type);
		return bind_data;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function = GetAggregate(arguments[0]->return_type);
		return BindQuantile(context, function, arguments);
	}
};

struct DiscreteQuantileFunction {
	static AggregateFunction GetAggregate(const LogicalType &type) {
		auto fun = GetDiscreteQuantile(type);
		fun.name = "quantile_disc";
		fun.bind = Bind;
		fun.serialize = QuantileBindData::Serialize;
		fun.deserialize = Deserialize;
		// temporarily push an argument so we can bind the actual quantile
		fun.arguments.emplace_back(LogicalType::DOUBLE);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return fun;
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto bind_data = QuantileBindData::Deserialize(deserializer, function);
		auto &quantile_data = bind_data->Cast<QuantileBindData>();

		auto &input_type = function.arguments[0];
		if (quantile_data.quantiles.size() == 1) {
			function = GetAggregate(input_type);
		} else {
			function = DiscreteQuantileListFunction::GetAggregate(input_type);
		}
		return bind_data;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function = GetAggregate(arguments[0]->return_type);
		return BindQuantile(context, function, arguments);
	}
};

struct ContinuousQuantileFunction {
	static AggregateFunction GetAggregate(const LogicalType &type) {
		auto fun = GetContinuousQuantile(type);
		fun.name = "quantile_cont";
		fun.bind = Bind;
		fun.serialize = QuantileBindData::Serialize;
		fun.deserialize = Deserialize;
		// temporarily push an argument so we can bind the actual quantile
		fun.arguments.emplace_back(LogicalType::DOUBLE);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return fun;
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto bind_data = QuantileBindData::Deserialize(deserializer, function);

		auto &input_type = function.arguments[0];
		function = GetAggregate(input_type);
		return bind_data;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function = GetAggregate(function.arguments[0].id() == LogicalTypeId::DECIMAL ? arguments[0]->return_type
		                                                                             : function.arguments[0]);
		return BindQuantile(context, function, arguments);
	}
};

struct ContinuousQuantileListFunction {
	static AggregateFunction GetAggregate(const LogicalType &type) {
		auto fun = GetContinuousQuantileList(type);
		fun.name = "quantile_cont";
		fun.bind = Bind;
		fun.serialize = QuantileBindData::Serialize;
		fun.deserialize = Deserialize;
		// temporarily push an argument so we can bind the actual quantile
		auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
		fun.arguments.push_back(list_of_double);
		fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return fun;
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto bind_data = QuantileBindData::Deserialize(deserializer, function);

		auto &input_type = function.arguments[0];
		function = GetAggregate(input_type);
		return bind_data;
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function = GetAggregate(function.arguments[0].id() == LogicalTypeId::DECIMAL ? arguments[0]->return_type
		                                                                             : function.arguments[0]);
		return BindQuantile(context, function, arguments);
	}
};

template <class OP>
AggregateFunction EmptyQuantileFunction(LogicalType input, LogicalType result, const LogicalType &extra_arg) {
	AggregateFunction fun({std::move(input)}, std::move(result), nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      OP::Bind);
	if (extra_arg.id() != LogicalTypeId::INVALID) {
		fun.arguments.push_back(extra_arg);
	}
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = OP::Deserialize;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunctionSet MedianFun::GetFunctions() {
	AggregateFunctionSet set("median");
	set.AddFunction(EmptyQuantileFunction<MedianFunction>(LogicalType::ANY, LogicalType::ANY, LogicalTypeId::INVALID));
	return set;
}

AggregateFunctionSet QuantileDiscFun::GetFunctions() {
	AggregateFunctionSet set("quantile_disc");
	set.AddFunction(
	    EmptyQuantileFunction<DiscreteQuantileFunction>(LogicalType::ANY, LogicalType::ANY, LogicalType::DOUBLE));
	set.AddFunction(EmptyQuantileFunction<DiscreteQuantileListFunction>(LogicalType::ANY, LogicalType::ANY,
	                                                                    LogicalType::LIST(LogicalType::DOUBLE)));
	// this function is here for deserialization - it cannot be called by users
	set.AddFunction(
	    EmptyQuantileFunction<DiscreteQuantileFunction>(LogicalType::ANY, LogicalType::ANY, LogicalType::INVALID));
	return set;
}

vector<LogicalType> GetContinuousQuantileTypes() {
	return {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,      LogicalType::BIGINT,
	        LogicalType::HUGEINT,   LogicalType::FLOAT,    LogicalType::DOUBLE,       LogicalType::DATE,
	        LogicalType::TIMESTAMP, LogicalType::TIME,     LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ};
}

AggregateFunctionSet QuantileContFun::GetFunctions() {
	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(EmptyQuantileFunction<ContinuousQuantileFunction>(
	    LogicalTypeId::DECIMAL, LogicalTypeId::DECIMAL, LogicalType::DOUBLE));
	quantile_cont.AddFunction(EmptyQuantileFunction<ContinuousQuantileListFunction>(
	    LogicalTypeId::DECIMAL, LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)));
	for (const auto &type : GetContinuousQuantileTypes()) {
		quantile_cont.AddFunction(EmptyQuantileFunction<ContinuousQuantileFunction>(type, type, LogicalType::DOUBLE));
		quantile_cont.AddFunction(
		    EmptyQuantileFunction<ContinuousQuantileListFunction>(type, type, LogicalType::LIST(LogicalType::DOUBLE)));
	}
	return quantile_cont;
}

} // namespace duckdb
