#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include <algorithm>

using namespace std;

namespace duckdb {

struct median_state_t {
	data_ptr_t v;
	idx_t len;
	idx_t pos;
};

struct QuantileBindData : public FunctionData {
	QuantileBindData(float quantile_) : quantile(quantile_) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantile);
	}

	float quantile;
};

template <class T> struct MedianOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
	}

	static void resize_state(median_state_t *state, idx_t new_len) {
		if (new_len <= state->len) {
			return;
		}
		median_state_t old_state;
		old_state.pos = 0;
		if (state->pos > 0) {
			old_state = *state;
		}
		// growing conservatively here since we could be running this on many small groups
		state->len = new_len;
		state->v = (data_ptr_t) new T[state->len];
		if (old_state.pos > 0) {
			memcpy(state->v, old_state.v, old_state.pos * sizeof(T));
			Destroy(&old_state);
		}
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
		if (state->pos == state->len) {
			resize_state(state, state->len == 0 ? 1 : state->len * 2);
		}
		D_ASSERT(state->v);
		((T *)state->v)[state->pos++] = data[idx];
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (target->len == 0) {
			*target = source;
			return;
		}
		resize_state(target, target->pos + source.pos);
		memcpy(target->v + target->pos, source.v, source.pos);
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
		idx_t offset = (idx_t)((double)(state->pos - 1) * bind_data->quantile);
		auto v_t = (T *)state->v;
		std::nth_element(v_t, v_t + offset, v_t + state->pos);
		target[idx] = v_t[offset];
		Destroy(state);
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->v) {
			delete[] state->v;
			state->v = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction GetQuantileAggregate(PhysicalType type) {
	switch (type) {

	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregate<median_state_t, int8_t, int8_t, MedianOperation<int8_t>>(
		    LogicalType::TINYINT, LogicalType::TINYINT);

	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregate<median_state_t, int16_t, int16_t, MedianOperation<int16_t>>(
		    LogicalType::SMALLINT, LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregate<median_state_t, int32_t, int32_t, MedianOperation<int32_t>>(
		    LogicalType::INTEGER, LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregate<median_state_t, int64_t, int64_t, MedianOperation<int64_t>>(
		    LogicalType::BIGINT, LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregate<median_state_t, hugeint_t, hugeint_t, MedianOperation<hugeint_t>>(
		    LogicalType::HUGEINT, LogicalType::HUGEINT);

	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregate<median_state_t, float, float, MedianOperation<float>>(
		    LogicalType::FLOAT, LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregate<median_state_t, double, double, MedianOperation<double>>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

unique_ptr<FunctionData> bind_median(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {
	function = GetQuantileAggregate(arguments[0]->return_type.InternalType());
	function.name = "median";
	return make_unique<QuantileBindData>(0.5);
}

unique_ptr<FunctionData> bind_quantile(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in range [0, 1]");
	}
	function = GetQuantileAggregate(arguments[0]->return_type.InternalType());
	function.name = "quantile";
	return make_unique<QuantileBindData>(quantile);
}

void QuantileFun::RegisterFunction(BuiltinFunctions &set) {

	AggregateFunctionSet median("median");
	median.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, bind_median));
	set.AddFunction(median);

	AggregateFunctionSet quantile("quantile");
	quantile.AddFunction(AggregateFunction({LogicalType::ANY, LogicalType::FLOAT}, LogicalType::ANY, nullptr, nullptr,
	                                       nullptr, nullptr, nullptr, nullptr, bind_quantile));
	set.AddFunction(quantile);
}

} // namespace duckdb
