#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "approximate_quantile.hpp"
#include <algorithm>
#include <cmath>
#include <stdlib.h>

namespace duckdb{

struct ApproxQuantileBindData : public FunctionData {
	ApproxQuantileBindData(float level, float error) : level(level), error(error) {
	}

	ApproxQuantileBindData(vector<float> &parameters) : ApproxQuantileBindData(parameters[0], parameters[1]) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ApproxQuantileBindData>(level, error);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (ApproxQuantileBindData &)other_p;
		return level == other.level && error == other.error;
	}

	float level;
	float error;
};


template <class T> struct approx_quantile_state_t {
	CSummary<T>* S{};
//	data_ptr_t v{};
	idx_t len{};
	idx_t pos{};
};

template <class T> struct ApproxQuantileOperation {

	template <class STATE> static void Initialize(STATE *state) {
//		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
		state->S = new CSummary<T>();
		int L = static_cast<int>(floor(log(10000)/log(2))+2);
		state->S->init(L,0.01);
	}

//	static void resize_state(approx_quantile_state_t<T> *state, idx_t new_len) {
//		if (new_len <= state->len) {
//			return;
//		}
//		state->v = (data_ptr_t)realloc(state->v, new_len * sizeof(T));
//		if (!state->v) {
//			throw InternalException("Memory allocation failure");
//		}
//		state->len = new_len;
//	}

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
//		if (state->pos == state->len) {
//			// growing conservatively here since we could be running this on many small groups
//			resize_state(state, state->len == 0 ? 1 : state->len * 2);
//		}
//		D_ASSERT(state->v);
		state->S->update(data[idx]);
		state->pos++;
//		((T *)state->v)[state->pos++] = data[idx];
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
//		if (source.pos == 0) {
//			return;
//		}
//		resize_state(target, target->pos + source.pos);
//		memcpy(target->v + target->pos * sizeof(T), source.v, source.pos * sizeof(T));
//		target->pos += source.pos;
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
		auto bind_data = (ApproxQuantileBindData *)bind_data_;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->level);

		target[idx] = state->S->query(offset) * 10000;
	}

	template <class STATE> static void Destroy(STATE *state) {
//		if (state->v) {
//			free(state->v);
//			state->v = nullptr;
//		}
//		if (state->S) {
//			delete state->S;
//			state->S = nullptr;
//		}
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

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<approx_quantile_state_t<hugeint_t>, hugeint_t, hugeint_t,
		                                                   ApproxQuantileOperation<hugeint_t>>(LogicalType::HUGEINT,
		                                                                                       LogicalType::HUGEINT);

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
	vector<float> parameters;
	for (size_t i = 1; i <= 2; i++) {
		if (!arguments[i]->IsScalar()) {
			throw BinderException("APPROX_QUANTILE can only take constant quantile parameters");
		}
		Value quantile_level_val = ExpressionExecutor::EvaluateScalar(*arguments[i]);
		auto quantile_level = quantile_level_val.GetValue<float>();

		if (quantile_level_val.is_null || quantile_level < 0 || quantile_level > 1) {
			throw BinderException("QUANTILE can only take parameters in range [0, 1]");
		}
		parameters.push_back(quantile_level);
		// remove the quantile argument so we can use the unary aggregate
		arguments.pop_back();
	}
	return make_unique<ApproxQuantileBindData>(parameters);
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
	approx_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT, LogicalType::FLOAT},
	                                              LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                              nullptr, bind_approx_quantile_decimal));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT16));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT32));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT64));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT128));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::DOUBLE));

	set.AddFunction(approx_quantile);
}

} // namespace duckdb
