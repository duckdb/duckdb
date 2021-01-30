#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "pcg_random.hpp"

#include <algorithm>
#include <cmath>
#include <random>
#include <stdlib.h>

namespace duckdb {

struct quantile_state_t {
	data_ptr_t v;
	idx_t len;
	idx_t pos;
	idx_t input_pos;
};

typedef struct {
  uint64_t state;
  uint64_t inc;
} pcg32_random_t;

pcg32_random_t gstate = {0x853c49e6748fea9bULL, 0xda3e39cb94b95bdbULL};

uint32_t pcg32_random_r(pcg32_random_t *rng) {
  uint64_t oldstate = rng->state;
  // Advance internal state
  rng->state = oldstate * 6364136223846793005ULL + (rng->inc | 1);
  // Calculate output function (XSH RR), uses old state for max ILP
  uint32_t xorshifted = ((oldstate >> 18u) ^ oldstate) >> 27u;
  uint32_t rot = oldstate >> 59u;
  return (xorshifted >> rot) | (xorshifted << ((-rot) & 31));
}

uint32_t random_bounded(uint32_t range) {
  uint64_t random32bit = pcg32_random_r(&gstate);
  uint64_t result = random32bit * range;
  return result >> 32;
}

struct QuantileBindData : public FunctionData {
	QuantileBindData(float quantile_, int32_t sample_size_) : quantile(quantile_), sample_size(sample_size_) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<QuantileBindData>(quantile, sample_size);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (QuantileBindData &)other_p;
		return quantile == other.quantile;
	}

	float quantile;
	int32_t sample_size;
};

template <class T> struct QuantileOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
		state->input_pos = 0;
	}

	static void resize_state(quantile_state_t *state, idx_t new_len) {
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
		auto bind_data = (QuantileBindData *)bind_data_;
		if (nullmask[idx]) {
			return;
		}
		if (state->pos == state->len) {
			// growing conservatively here since we could be running this on many small groups
			resize_state(state, state->len == 0 ? 1 : state->len * 2);
		}
		D_ASSERT(state->v);
		if (bind_data->sample_size != -1) {
            //! We gotta do some sampling
            if (state->pos < bind_data->sample_size) {
				((T *)state->v)[state->pos++] = data[idx];
			} else {
				size_t nextpos = random_bounded(state->input_pos);
				if (nextpos < bind_data->sample_size){
					((T *)state->v)[nextpos] = data[idx];
				}
			}
		state->input_pos++;
		}
		else{
			((T *)state->v)[state->pos++] = data[idx];
		}

	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (source.pos == 0) {
			return;
		}
		resize_state(target, target->pos + source.pos);
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

	template <class STATE> static void Destroy(STATE *state) {
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
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, int16_t, int16_t,
		                                                   QuantileOperation<int16_t>>(LogicalType::SMALLINT,
		                                                                               LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, int32_t, int32_t,
		                                                   QuantileOperation<int32_t>>(LogicalType::INTEGER,
		                                                                               LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, int64_t, int64_t,
		                                                   QuantileOperation<int64_t>>(LogicalType::BIGINT,
		                                                                               LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, hugeint_t, hugeint_t,
		                                                   QuantileOperation<hugeint_t>>(LogicalType::HUGEINT,
		                                                                                 LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, float, float, QuantileOperation<float>>(
		    LogicalType::FLOAT, LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<quantile_state_t, double, double, QuantileOperation<double>>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

unique_ptr<FunctionData> bind_median(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {
	return make_unique<QuantileBindData>(0.5, 100);
}

unique_ptr<FunctionData> bind_median_decimal(ClientContext &context, AggregateFunction &function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = bind_median(context, function, arguments);

	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "median";
	return bind_data;
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
	arguments.pop_back();
	return make_unique<QuantileBindData>(quantile, -1);
}

unique_ptr<FunctionData> bind_reservoir_quantile(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in range [0, 1]");
	}
	if (arguments.size() <= 2) {
		arguments.pop_back();
		return make_unique<QuantileBindData>(quantile, 8192);
	}
	if (!arguments[2]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value sample_size_val = ExpressionExecutor::EvaluateScalar(*arguments[2]);
	auto sample_size = sample_size_val.GetValue<int32_t>();

	if (sample_size_val.is_null || sample_size <= 0 ) {
		throw BinderException("Percentage of the sample must be bigger than 0");
	}

	// remove the quantile argument so we can use the unary aggregate
	arguments.pop_back();
	arguments.pop_back();
	return make_unique<QuantileBindData>(quantile, sample_size);
}

unique_ptr<FunctionData> bind_quantile_decimal(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = bind_quantile(context, function, arguments);
	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "quantile";
	return bind_data;
}

unique_ptr<FunctionData> bind_reservoir_quantile_decimal(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = bind_reservoir_quantile(context, function, arguments);
	function = GetQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "reservoir_quantile";
	return bind_data;
}

AggregateFunction GetMedianAggregate(PhysicalType type) {
	auto fun = GetQuantileAggregateFunction(type);
	fun.bind = bind_median;
	return fun;
}

AggregateFunction GetQuantileAggregate(PhysicalType type) {
	auto fun = GetQuantileAggregateFunction(type);
	fun.bind = bind_quantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

AggregateFunction GetReservoirQuantileAggregate(PhysicalType type) {
	auto fun = GetQuantileAggregateFunction(type);
	fun.bind = bind_reservoir_quantile;
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
	quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT},
	                                       LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                       bind_quantile_decimal));

	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT16));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT32));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT64));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::INT128));
	quantile.AddFunction(GetQuantileAggregate(PhysicalType::DOUBLE));

	set.AddFunction(quantile);

	AggregateFunctionSet reservoir_quantile("reservoir_quantile");
	reservoir_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT, LogicalType::INTEGER},
	                                       LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                       bind_reservoir_quantile_decimal));
    reservoir_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT},
	                                       LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                       bind_reservoir_quantile_decimal));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT16));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT32));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT64));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT128));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::DOUBLE));

	set.AddFunction(reservoir_quantile);
}

} // namespace duckdb
