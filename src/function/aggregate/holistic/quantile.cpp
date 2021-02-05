#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
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
	explicit QuantileBindData(float quantile_p) : quantile(quantile_p) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantile);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (QuantileBindData &)other_p;
		return quantile == other.quantile;
	}

	float quantile;
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
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data_, INPUT_TYPE *data, nullmask_t &nullmask, idx_t idx) {
		if (nullmask[idx]) {
			return;
		}
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
	static void Finalize(Vector &result, FunctionData *bind_data_, STATE *state, TARGET_TYPE *target,
	                     nullmask_t &nullmask, idx_t idx) {
		if (state->pos == 0) {
			nullmask[idx] = true;
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_);
		auto bind_data = (QuantileBindData *)bind_data_;
		auto v_t = (T *)state->v;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->quantile);
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
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int16_t, int16_t,
		                                                   QuantileOperation<int16_t>>(LogicalType::SMALLINT,
		                                                                               LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int32_t, int32_t,
		                                                   QuantileOperation<int32_t>>(LogicalType::INTEGER,
		                                                                               LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<QuantileState, int64_t, int64_t,
		                                                   QuantileOperation<int64_t>>(LogicalType::BIGINT,
		                                                                               LogicalType::BIGINT);

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

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {
	return make_unique<QuantileBindData>(0.5);
}

unique_ptr<FunctionData> bind_median_decimal(ClientContext &context, AggregateFunction &function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "median";
	return bind_data;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in range [0, 1]");
	}
	arguments.pop_back();
	return make_unique<QuantileBindData>(quantile);
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

void QuantileFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalType::DECIMAL}, LogicalType::DECIMAL, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, bind_median_decimal));
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

	set.AddFunction(quantile);
}

} // namespace duckdb
