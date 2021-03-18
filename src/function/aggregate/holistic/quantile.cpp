#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "pcg_random.hpp"

#include <algorithm>
#include <cmath>
#include <queue>
#include <random>
#include <stdlib.h>
#include <utility>

namespace duckdb {

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
		auto v_t = (T *)state->v;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state->pos);
		target[idx] = v_t[offset];
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

AggregateFunction GetQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int16_t, int16_t, QuantileOperation<int16_t>>(
		    LogicalType::SMALLINT, LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int32_t, int32_t, QuantileOperation<int32_t>>(
		    LogicalType::INTEGER, LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int64_t, int64_t, QuantileOperation<int64_t>>(
		    LogicalType::BIGINT, LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, hugeint_t, hugeint_t,
		                                                   QuantileOperation<hugeint_t>>(LogicalType::HUGEINT,
		                                                                                 LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, float, float, QuantileOperation<float>>(
		    LogicalType::FLOAT, LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, double, double, QuantileOperation<double>>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

template <class STATE_TYPE, class INPUT_TYPE, class TARGET_TYPE>
static void QuantileListFinalize(Vector &result_list, FunctionData *bind_data_p, STATE_TYPE *state,
                                 TARGET_TYPE *target, ValidityMask &mask, idx_t idx) {
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
		ListVector::PushBack(result_list,val);
	}
	target[idx].length = bind_data->quantiles.size();
}

template <class STATE_TYPE, class INPUT_TYPE, class RESULT_TYPE>
static void ExecuteQuantileListFinalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(result.GetType().child_types().size() == 1);

	auto list_child = make_unique<Vector>(result.GetType().child_types()[0].second);
	ListVector::SetEntry(result, move(list_child));

	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &mask = ConstantVector::Validity(result);
		QuantileListFinalize<STATE_TYPE, INPUT_TYPE, RESULT_TYPE>(result, bind_data, sdata[0], rdata, mask, 0);
	} else {
		D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			QuantileListFinalize<STATE_TYPE, INPUT_TYPE, RESULT_TYPE>(result, bind_data, sdata[i], rdata, mask, i);
		}
	}


	result.Verify(count);
}

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type) {
	LogicalType result_type(LogicalTypeId::LIST, {{"", input_type}});
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    ExecuteQuantileListFinalize<STATE, INPUT_TYPE, RESULT_TYPE>,
	    AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
}

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregateDestructor(const LogicalType &input_type) {
	auto aggregate = QuantileListAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP>(input_type);
	aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	return aggregate;
}

AggregateFunction GetQuantileListAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return QuantileListAggregateDestructor<QuantileState, int16_t, list_entry_t, QuantileOperation<int16_t>>(
		    LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return QuantileListAggregateDestructor<QuantileState, int32_t, list_entry_t, QuantileOperation<int32_t>>(
		    LogicalType::INTEGER);

	case PhysicalType::INT64:
		return QuantileListAggregateDestructor<QuantileState, int64_t, list_entry_t, QuantileOperation<int64_t>>(
		    LogicalType::BIGINT);

	case PhysicalType::INT128:
		return QuantileListAggregateDestructor<QuantileState, hugeint_t, list_entry_t, QuantileOperation<hugeint_t>>(
		    LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return QuantileListAggregateDestructor<QuantileState, float, list_entry_t, QuantileOperation<float>>(
		    LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return QuantileListAggregateDestructor<QuantileState, double, list_entry_t, QuantileOperation<double>>(
		    LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_unique<QuantileBindData>(0.5);
}

unique_ptr<FunctionData> BindMedianDecimal(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "median";
	return bind_data;
}

static float CheckQuantile(const Value &quantile_val) {
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	vector<float> quantiles;
	if (quantile_val.type().IsNumeric()) {
		quantiles.push_back(CheckQuantile(quantile_val));
	} else {
		for (const auto &element_val : quantile_val.list_value) {
			quantiles.push_back(CheckQuantile(element_val));
		}
	}

	arguments.pop_back();
	return make_unique<QuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "quantile";
	return bind_data;
}

AggregateFunction GetMedianAggregate(PhysicalType type) {
	auto fun = GetQuantileAggregateFunction(type);
	fun.bind = BindMedian;
	return fun;
}

AggregateFunction GetQuantileAggregate(PhysicalType type) {
	auto fun = GetQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetQuantileListAggregate(PhysicalType type) {
	auto fun = GetQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	// temporarily push an argument so we can bind the actual quantile
	LogicalType list_of_float(LogicalTypeId::LIST, {std::make_pair("", LogicalType::FLOAT)});
	fun.arguments.push_back(list_of_float);
	return fun;
}

void QuantileFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalType::DECIMAL}, LogicalType::DECIMAL, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, BindMedianDecimal));
	median.AddFunction(GetMedianAggregate(PhysicalType::INT16));
	median.AddFunction(GetMedianAggregate(PhysicalType::INT32));
	median.AddFunction(GetMedianAggregate(PhysicalType::INT64));
	median.AddFunction(GetMedianAggregate(PhysicalType::INT128));
	median.AddFunction(GetMedianAggregate(PhysicalType::DOUBLE));

	set.AddFunction(median);

	AggregateFunctionSet quantile("quantile");
	quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT}, LogicalType::DECIMAL, nullptr,
	                                       nullptr, nullptr, nullptr, nullptr, nullptr, BindQuantileDecimal));

	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT16));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT32));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT64));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT128));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::DOUBLE));

	// LIST variants
	quantile.AddFunction(GetQuantileListAggregate(PhysicalType::INT16));
	quantile.AddFunction(GetQuantileListAggregate(PhysicalType::INT32));
	quantile.AddFunction(GetQuantileListAggregate(PhysicalType::INT64));
	quantile.AddFunction(GetQuantileListAggregate(PhysicalType::INT128));
	quantile.AddFunction(GetQuantileListAggregate(PhysicalType::DOUBLE));

	set.AddFunction(quantile);
}

} // namespace duckdb
