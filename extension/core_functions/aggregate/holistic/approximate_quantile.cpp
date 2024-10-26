#include "duckdb/execution/expression_executor.hpp"
#include "core_functions/aggregate/holistic_functions.hpp"
#include "t_digest.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <algorithm>
#include <cmath>
#include <stdlib.h>

namespace duckdb {

struct ApproxQuantileState {
	duckdb_tdigest::TDigest *h;
	idx_t pos;
};

struct ApproximateQuantileBindData : public FunctionData {
	ApproximateQuantileBindData() {
	}
	explicit ApproximateQuantileBindData(float quantile_p) : quantiles(1, quantile_p) {
	}

	explicit ApproximateQuantileBindData(vector<float> quantiles_p) : quantiles(std::move(quantiles_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ApproximateQuantileBindData>(quantiles);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ApproximateQuantileBindData>();
		//		return quantiles == other.quantiles;
		if (quantiles != other.quantiles) {
			return false;
		}
		return true;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<ApproximateQuantileBindData>();
		serializer.WriteProperty(100, "quantiles", bind_data.quantiles);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<ApproximateQuantileBindData>();
		deserializer.ReadProperty(100, "quantiles", result->quantiles);
		return std::move(result);
	}

	vector<float> quantiles;
};

struct ApproxQuantileOperation {
	using SAVE_TYPE = duckdb_tdigest::Value;

	template <class STATE>
	static void Initialize(STATE &state) {
		state.pos = 0;
		state.h = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto val = Cast::template Operation<INPUT_TYPE, SAVE_TYPE>(input);
		if (!Value::DoubleIsFinite(val)) {
			return;
		}
		if (!state.h) {
			state.h = new duckdb_tdigest::TDigest(100);
		}
		state.h->add(val);
		state.pos++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.pos == 0) {
			return;
		}
		D_ASSERT(source.h);
		if (!target.h) {
			target.h = new duckdb_tdigest::TDigest(100);
		}
		target.h->merge(source.h);
		target.pos += source.pos;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.h) {
			delete state.h;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ApproxQuantileScalarOperation : public ApproxQuantileOperation {
	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(state.h);
		D_ASSERT(finalize_data.input.bind_data);
		state.h->compress();
		auto &bind_data = finalize_data.input.bind_data->template Cast<ApproximateQuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		// The result is approximate, so clamp instead of overflowing.
		const auto source = state.h->quantile(bind_data.quantiles[0]);
		if (TryCast::Operation(source, target, false)) {
			return;
		} else if (source < 0) {
			target = NumericLimits<TARGET_TYPE>::Minimum();
		} else {
			target = NumericLimits<TARGET_TYPE>::Maximum();
		}
	}
};

static AggregateFunction GetApproximateQuantileAggregateFunction(const LogicalType &type) {
	//	Not binary comparable
	if (type == LogicalType::TIME_TZ) {
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, dtime_tz_t, dtime_tz_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	}
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int8_t, int8_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int16_t, int16_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int32_t, int32_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int64_t, int64_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, hugeint_t, hugeint_t,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, float, float,
		                                                   ApproxQuantileScalarOperation>(type, type);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, double, double,
		                                                   ApproxQuantileScalarOperation>(type, type);
	default:
		throw InternalException("Unimplemented quantile aggregate");
	}
}

static AggregateFunction GetApproximateQuantileDecimalAggregateFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return GetApproximateQuantileAggregateFunction(LogicalType::TINYINT);
	case PhysicalType::INT16:
		return GetApproximateQuantileAggregateFunction(LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return GetApproximateQuantileAggregateFunction(LogicalType::INTEGER);
	case PhysicalType::INT64:
		return GetApproximateQuantileAggregateFunction(LogicalType::BIGINT);
	case PhysicalType::INT128:
		return GetApproximateQuantileAggregateFunction(LogicalType::HUGEINT);
	default:
		throw InternalException("Unimplemented quantile decimal aggregate");
	}
}

static float CheckApproxQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("APPROXIMATE QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<float>();
	if (quantile < 0 || quantile > 1) {
		throw BinderException("APPROXIMATE QUANTILE can only take parameters in range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindApproxQuantile(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("APPROXIMATE QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (quantile_val.IsNull()) {
		throw BinderException("APPROXIMATE QUANTILE parameter list cannot be NULL");
	}

	vector<float> quantiles;
	switch (quantile_val.type().id()) {
	case LogicalTypeId::LIST:
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckApproxQuantile(element_val));
		}
		break;
	case LogicalTypeId::ARRAY:
		for (const auto &element_val : ArrayValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckApproxQuantile(element_val));
		}
		break;
	default:
		quantiles.push_back(CheckApproxQuantile(quantile_val));
		break;
	}

	// remove the quantile argument so we can use the unary aggregate
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<ApproximateQuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindApproxQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproximateQuantileDecimalAggregateFunction(arguments[0]->return_type);
	function.name = "approx_quantile";
	function.serialize = ApproximateQuantileBindData::Serialize;
	function.deserialize = ApproximateQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetApproximateQuantileAggregate(const LogicalType &type) {
	auto fun = GetApproximateQuantileAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::FLOAT);
	return fun;
}

template <class CHILD_TYPE>
struct ApproxQuantileListOperation : public ApproxQuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ApproximateQuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		D_ASSERT(state.h);
		state.h->compress();

		auto &entry = target;
		entry.offset = ridx;
		entry.length = bind_data.quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[ridx + q] = Cast::template Operation<SAVE_TYPE, CHILD_TYPE>(state.h->quantile(quantile));
		}

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction ApproxQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedApproxQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ApproxQuantileState;
	using OP = ApproxQuantileListOperation<INPUT_TYPE>;
	auto fun = ApproxQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	return fun;
}

AggregateFunction GetApproxQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedApproxQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
		return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::TIME_TZ:
		//	Not binary comparable
		return GetTypedApproxQuantileListAggregateFunction<dtime_tz_t, dtime_tz_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedApproxQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedApproxQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented approximate quantile list decimal aggregate");
		}
	default:
		throw NotImplementedException("Unimplemented approximate quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindApproxQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproxQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "approx_quantile";
	function.serialize = ApproximateQuantileBindData::Serialize;
	function.deserialize = ApproximateQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetApproxQuantileListAggregate(const LogicalType &type) {
	auto fun = GetApproxQuantileListAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_float = LogicalType::LIST(LogicalType::FLOAT);
	fun.arguments.push_back(list_of_float);
	return fun;
}

AggregateFunctionSet ApproxQuantileFun::GetFunctions() {
	AggregateFunctionSet approx_quantile;
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::FLOAT}, LogicalTypeId::DECIMAL,
	                                              nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                              BindApproxQuantileDecimal));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::SMALLINT));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::INTEGER));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::BIGINT));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::HUGEINT));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::DOUBLE));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::DATE));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::TIME));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::TIME_TZ));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::TIMESTAMP));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(LogicalType::TIMESTAMP_TZ));

	// List variants
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::FLOAT)},
	                                              LogicalType::LIST(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr,
	                                              nullptr, nullptr, nullptr, BindApproxQuantileDecimalList));

	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::TINYINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::SMALLINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::INTEGER));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::BIGINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::HUGEINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::FLOAT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::DOUBLE));

	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalType::DATE));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalType::TIME));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalType::TIME_TZ));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalType::TIMESTAMP));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalType::TIMESTAMP_TZ));

	return approx_quantile;
}

} // namespace duckdb
