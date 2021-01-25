#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "t_digest.hpp"
#include <algorithm>
#include <cmath>
#include <stdlib.h>

namespace duckdb{

template <class T> struct approx_quantile_state_t {
	td_histogram<T> *h = nullptr;
	idx_t pos{};
};

template <class T> struct ApproxQuantileOperation {

	template <class STATE> static void Initialize(STATE *state) {
		state->pos = 0;
		state->h = td_new<T>(8192);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *data, nullmask_t &nullmask, idx_t idx) {
		if (nullmask[idx]) {
			return;
		}
		D_ASSERT(state->h);
        td_add<T>(state->h, data[idx],1);
		state->pos++;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (source.pos == 0) {
			return;
		}
		td_merge(target->h,source.h);
		target->pos += source.pos;
	}


	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_, STATE *state, TARGET_TYPE *target,
	                     nullmask_t &nullmask, idx_t idx) {

		if (state->pos == 0) {
			nullmask[idx] = true;
			return;
		}
		D_ASSERT(state->h);
		D_ASSERT(bind_data_);
		auto bind_data = (QuantileBindData *)bind_data_;
		target[idx] = td_value_at(state->h, bind_data->quantile);
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->h) {
			td_free<T>(state->h);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction GetApproximateQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<int16_t>, int16_t, int16_t,
		                                                   ApproxQuantileOperation<int16_t>>(LogicalType::SMALLINT,
		                                                                                     LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<int32_t>, int32_t, int32_t,
		                                                   ApproxQuantileOperation<int32_t>>(LogicalType::INTEGER,
		                                                                                     LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<int64_t>, int64_t, int64_t,
		                                                   ApproxQuantileOperation<int64_t>>(LogicalType::BIGINT,
		                                                                                     LogicalType::BIGINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<float>, float, float,
		                                                   ApproxQuantileOperation<float>>(LogicalType::FLOAT,
		                                                                                   LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<double>, double, double,
		                                                   ApproxQuantileOperation<double>>(LogicalType::DOUBLE,
		                                                                                    LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

unique_ptr<FunctionData> bind_approx_quantile(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("APPROXIMATE QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("APPROXIMATE QUANTILE can only take parameters in range [0, 1]");
	}
	// remove the quantile argument so we can use the unary aggregate
	arguments.pop_back();
	return make_unique<QuantileBindData>(quantile);
}

unique_ptr<FunctionData> bind_approx_quantile_decimal(ClientContext &context, AggregateFunction &function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = bind_approx_quantile(context, function, arguments);
	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "approx_quantile";
	return bind_data;
}

AggregateFunction GetApproximateQuantileAggregate(PhysicalType type) {
	auto fun = GetApproximateQuantileAggregateFunction(type);
	fun.bind = bind_quantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

void ApproximateQuantileFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet approx_quantile("approx_quantile");
	approx_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT},
	                                              LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                              nullptr, bind_approx_quantile_decimal));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT16));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT32));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT64));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::DOUBLE));

	set.AddFunction(approx_quantile);
}

} // namespace duckdb
