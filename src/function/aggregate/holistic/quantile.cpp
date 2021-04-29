#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <algorithm>
#include <queue>
#include <stdlib.h>
#include <utility>

namespace duckdb {

bool operator<(const interval_t &lhs, const interval_t &rhs) {
	return LessThan::Operation(lhs, rhs);
}

template <>
timestamp_t Cast::Operation(date_t date) {
	return Timestamp::FromDatetime(date, dtime_t(0));
}

struct QuantileState {
	data_ptr_t v;
	idx_t len;
	idx_t pos;
};

struct QuantileBindData : public FunctionData {
	explicit QuantileBindData(float quantile_p) : quantiles(1, quantile_p) {
	}

	explicit QuantileBindData(const vector<float> &quantiles_p) : quantiles(quantiles_p) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantiles);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (QuantileBindData &)other_p;
		return quantiles == other.quantiles;
	}

	vector<float> quantiles;
};

template <class T>
struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
	}

	static void ResizeState(QuantileState *state, idx_t new_len) {
		if (new_len <= state->len) {
			return;
		}
		state->v = (data_ptr_t)realloc(state->v, new_len * sizeof(T));
		if (!state->v) {
			throw InternalException("Memory allocation failure");
		}
		state->len = new_len;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, mask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data_p, INPUT_TYPE *data, ValidityMask &mask, idx_t idx) {
		if (state->pos == state->len) {
			// growing conservatively here since we could be running this on many small groups
			ResizeState(state, state->len == 0 ? 1 : state->len * 2);
		}
		D_ASSERT(state->v);
		((T *)state->v)[state->pos++] = data[idx];
	}

	template <class STATE, class OP>
	static void Combine(STATE source, STATE *target) {
		if (source.pos == 0) {
			return;
		}
		ResizeState(target, target->pos + source.pos);
		memcpy(target->v + target->pos * sizeof(T), source.v, source.pos * sizeof(T));
		target->pos += source.pos;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->v) {
			free(state->v);
			state->v = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class STATE_TYPE, class RESULT_TYPE, class OP>
static void ExecuteListFinalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(result.GetType().child_types().size() == 1);

	auto list_child = make_unique<Vector>(result.GetType().child_types()[0].second);
	ListVector::SetEntry(result, move(list_child));

	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &mask = ConstantVector::Validity(result);
		OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[0], rdata, mask, 0);
	} else {
		D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata, mask, i);
		}
	}

	result.Verify(count);
}

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type(LogicalTypeId::LIST, {{"", child_type}});
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    ExecuteListFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>, nullptr,
	    AggregateFunction::StateDestroy<STATE, OP>);
}

template <class INPUT_TYPE>
struct DiscreteQuantileOperation : public QuantileOperation<INPUT_TYPE> {

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, TARGET_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		auto v_t = (INPUT_TYPE *)state->v;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state->pos);
		target[idx] = v_t[offset];
	}
};

template <typename INPUT_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	return AggregateFunction::UnaryAggregateDestructor<QuantileState, INPUT_TYPE, INPUT_TYPE,
	                                                   DiscreteQuantileOperation<INPUT_TYPE>>(type, type);
}

AggregateFunction GetDiscreteQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileAggregateFunction<int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileAggregateFunction<int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileAggregateFunction<hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileAggregateFunction<float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileAggregateFunction<double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileAggregateFunction<int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileAggregateFunction<hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileAggregateFunction<interval_t>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile aggregate");
	}
}

template <class INPUT_TYPE>
struct DiscreteQuantileListOperation : public QuantileOperation<INPUT_TYPE> {

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, TARGET_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		target[idx].offset = ListVector::GetListSize(result_list);
		auto v_t = (INPUT_TYPE *)state->v;
		for (const auto &quantile : bind_data->quantiles) {
			auto offset = (idx_t)((double)(state->pos - 1) * quantile);
			std::nth_element(v_t, v_t + offset, v_t + state->pos);
			auto val = Value::CreateValue(v_t[offset]);
			ListVector::PushBack(result_list, val);
		}
		target[idx].length = bind_data->quantiles.size();
	}
};

template <typename INPUT_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	return QuantileListAggregate<QuantileState, INPUT_TYPE, list_entry_t, DiscreteQuantileListOperation<INPUT_TYPE>>(
	    type, type);
}

AggregateFunction GetDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileListAggregateFunction<int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileListAggregateFunction<float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileListAggregateFunction<double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileListAggregateFunction<int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileListAggregateFunction<int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileListAggregateFunction<int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t>(type);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t>(type);
	case LogicalTypeId::TIME:
		return GetTypedDiscreteQuantileListAggregateFunction<dtime_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileListAggregateFunction<interval_t>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <class INPUT_TYPE, class TARGET_TYPE>
static TARGET_TYPE Interpolate(INPUT_TYPE *v_t, const float q, const idx_t n) {
	const auto RN = ((double)(n - 1) * q);
	const auto FRN = idx_t(floor(RN));
	const auto CRN = idx_t(ceil(RN));

	if (CRN == FRN) {
		std::nth_element(v_t, v_t + FRN, v_t + n);
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[FRN]);
	} else {
		std::nth_element(v_t, v_t + FRN, v_t + n);
		std::nth_element(v_t + FRN, v_t + CRN, v_t + n);
		auto lo = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[FRN]);
		auto hi = Cast::Operation<INPUT_TYPE, TARGET_TYPE>(v_t[CRN]);
		auto delta = hi - lo;
		return lo + delta * (RN - FRN);
	}
}

template <class INPUT_TYPE>
struct ContinuousQuantileOperation : public QuantileOperation<INPUT_TYPE> {

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, TARGET_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		auto v_t = (INPUT_TYPE *)state->v;
		target[idx] = Interpolate<INPUT_TYPE, TARGET_TYPE>(v_t, bind_data->quantiles[0], state->pos);
	}
};

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileAggregateFunction(const LogicalType &input_type,
                                                              const LogicalType &target_type) {
	return AggregateFunction::UnaryAggregateDestructor<QuantileState, INPUT_TYPE, TARGET_TYPE,
	                                                   ContinuousQuantileOperation<INPUT_TYPE>>(input_type,
	                                                                                            target_type);
}

AggregateFunction GetContinuousQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
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
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <class INPUT_TYPE, class CHILD_TYPE>
struct ContinuousQuantileListOperation : public QuantileOperation<INPUT_TYPE> {

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, TARGET_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_p);
		auto bind_data = (QuantileBindData *)bind_data_p;
		target[idx].offset = ListVector::GetListSize(result_list);
		auto v_t = (INPUT_TYPE *)state->v;
		for (const auto &quantile : bind_data->quantiles) {
			auto child = Interpolate<INPUT_TYPE, CHILD_TYPE>(v_t, quantile, state->pos);
			auto val = Value::CreateValue(child);
			ListVector::PushBack(result_list, val);
		}
		target[idx].length = bind_data->quantiles.size();
	}
};

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &target_type) {
	return QuantileListAggregate<QuantileState, INPUT_TYPE, list_entry_t,
	                             ContinuousQuantileListOperation<INPUT_TYPE, TARGET_TYPE>>(input_type, target_type);
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
			return GetTypedContinuousQuantileListAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileListAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileListAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileListAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
		return GetTypedContinuousQuantileListAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
		return GetTypedContinuousQuantileListAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_unique<QuantileBindData>(0.5);
}

unique_ptr<FunctionData> BindMedianDecimal(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "median";
	return bind_data;
}

static float CheckQuantile(const Value &quantile_val) {
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	vector<float> quantiles;
	if (quantile_val.type().child_types().empty()) {
		quantiles.push_back(CheckQuantile(quantile_val));
	} else {
		for (const auto &element_val : quantile_val.list_value) {
			quantiles.push_back(CheckQuantile(element_val));
		}
	}

	arguments.pop_back();
	return make_unique<QuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_disc";
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetContinuousQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_cont";
	return bind_data;
}

AggregateFunction GetMedianAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindMedian;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	LogicalType list_of_float(LogicalTypeId::LIST, {std::make_pair("", LogicalType::FLOAT)});
	fun.arguments.push_back(list_of_float);
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	LogicalType list_of_double(LogicalTypeId::LIST, {std::make_pair("", LogicalType::DOUBLE)});
	fun.arguments.push_back(list_of_double);
	return fun;
}

void QuantileFun::RegisterFunction(BuiltinFunctions &set) {
	const vector<LogicalType> QUANTILES = {LogicalType::TINYINT, LogicalType::SMALLINT, LogicalType::INTEGER,
	                                       LogicalType::BIGINT,  LogicalType::HUGEINT,  LogicalType::FLOAT,
	                                       LogicalType::DOUBLE,  LogicalType::DATE,     LogicalType::TIMESTAMP,
	                                       LogicalType::TIME,    LogicalType::INTERVAL};

	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalType::DECIMAL}, LogicalType::DECIMAL, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, BindMedianDecimal));

	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT}, LogicalType::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindDiscreteQuantileDecimal));

	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT}, LogicalType::DECIMAL,
	                                            nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                            BindContinuousQuantileDecimal));

	for (const auto &type : QUANTILES) {
		median.AddFunction(GetMedianAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
		if (type.id() != LogicalTypeId::INTERVAL) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
		}
	}

	set.AddFunction(median);
	set.AddFunction(quantile_disc);
	set.AddFunction(quantile_cont);

	quantile_disc.name = "quantile";
	set.AddFunction(quantile_disc);
}

} // namespace duckdb
